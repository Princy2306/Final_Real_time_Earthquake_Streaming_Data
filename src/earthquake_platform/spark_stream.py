"""Kafka -> Bronze -> Silver -> Gold using Spark Structured Streaming and Delta."""
import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp, to_date,
    when, lit, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from delta import configure_spark_with_delta_pip

from earthquake_platform.clustering import candidate_clusters
from earthquake_platform.schemas import EARTHQUAKE_SCHEMA

LOG = logging.getLogger("earthquake-spark")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "earthquake-data")
ROOT = f"s3a://{BUCKET}/lakehouse"
CHECKPOINT = os.getenv("SPARK_CHECKPOINT", f"s3a://{BUCKET}/checkpoints/earthquakes")
MAX_DAYS = int(os.getenv("MAX_TIME_DAYS", "7"))
MAX_DISTANCE_KM = float(os.getenv("MAX_DISTANCE_KM", "100"))
MIN_CLUSTER_EVENTS = int(os.getenv("MINIMUM_CLUSTER_EVENTS", "2"))


def build_spark():
    builder = (
        SparkSession.builder.appName("EarthquakeLakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
    )

    extra_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
        "org.apache.hadoop:hadoop-aws:3.4.1",
    ]

    return configure_spark_with_delta_pip(
        builder,
        extra_packages=extra_packages
    ).getOrCreate()



def refresh_gold(silver_path, gold_cluster_path, gold_membership_path, spark):
    """Recompute candidate clusters from the Silver snapshot after each micro-batch."""
    try:
        rows = (
            spark.read.format("delta").load(silver_path)
            .where(col("event_time") >= expr(f"current_timestamp() - INTERVAL {MAX_DAYS} DAYS"))
            .where(col("event_type") == "earthquake")
            .select("id", "magnitude", "time", "longitude", "latitude", "depth", "place")
            .where(
               col("time").isNotNull()
               & col("longitude").isNotNull()
               & col("latitude").isNotNull()
            )
            .collect()
        ) 
    except Exception:
        LOG.exception("Could not read Silver table for Gold refresh")
        return

    events = [r.asDict(recursive=True) for r in rows]
    clusters = candidate_clusters(events, MAX_DAYS, MAX_DISTANCE_KM, MIN_CLUSTER_EVENTS)
    if not clusters:
        LOG.info("No candidate clusters currently meet the configured thresholds")
        spark.createDataFrame([], cluster_schema).write.format("delta").mode("overwrite").save(gold_cluster_path)
        spark.createDataFrame([], membership_schema).write.format("delta").mode("overwrite").save(gold_membership_path)
        return

    cluster_rows, membership_rows = [], []
    for cluster in clusters:
        cluster_id = str(cluster["mainshock_id"])
        cluster_rows.append({
            "cluster_id": cluster_id,
            "mainshock_id": cluster["mainshock_id"],
            "mainshock_magnitude": cluster["mainshock_magnitude"],
            "event_count": cluster["event_count"],
            "potential_aftershock_count": cluster["potential_aftershock_count"],
            "min_longitude": cluster["min_longitude"], "max_longitude": cluster["max_longitude"],
            "min_latitude": cluster["min_latitude"], "max_latitude": cluster["max_latitude"],
            "start_time_ms": cluster["start_time_ms"], "end_time_ms": cluster["end_time_ms"],
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
        })
        for event in cluster["events"]:
            membership_rows.append({
                "cluster_id": cluster_id,
                "event_id": event["id"],
                "magnitude": event.get("magnitude"),
                "event_time_ms": event.get("time"),
                "place": event.get("place"),
                "longitude": event.get("longitude"),
                "latitude": event.get("latitude"),
                "depth": event.get("depth"),
            })

    spark.createDataFrame(cluster_rows).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_cluster_path)
    spark.createDataFrame(membership_rows).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_membership_path)
    LOG.info("Gold refreshed: %d candidate clusters", len(cluster_rows))


def process_batch(batch_df, batch_id, spark):
    if batch_df.rdd.isEmpty():
        return
    bronze_path = f"{ROOT}/bronze/earthquakes"
    silver_path = f"{ROOT}/silver/earthquakes"
    gold_cluster_path = f"{ROOT}/gold/clusters"
    gold_membership_path = f"{ROOT}/gold/cluster_events"

    raw = batch_df.withColumn("ingest_batch_id", lit(batch_id))
    raw.write.format("delta").mode("append").save(bronze_path)

    cleaned = (
        batch_df
        .dropDuplicates(["id"])
        .filter(col("id").isNotNull())
        .withColumn("event_time", (col("time") / 1000).cast("timestamp"))
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("is_unusual_magnitude", when(col("magnitude") >= 7.0, True).otherwise(False))
        .filter(col("longitude").between(-180, 180))
        .filter(col("latitude").between(-90, 90))
        .filter(col("depth").isNull() | (col("depth") >= -10))
        .withColumn("processed_at", current_timestamp())
    )
    # Idempotent Silver upsert: Kafka may replay a message after a restart.
    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, silver_path):
        target = DeltaTable.forPath(spark, silver_path)
        (target.alias("t")
            .merge(cleaned.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        cleaned.write.format("delta").mode("overwrite").partitionBy("event_date").save(silver_path)

    refresh_gold(silver_path, gold_cluster_path, gold_membership_path, spark)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        kafka.select(from_json(col("value").cast("string"), EARTHQUAKE_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("kafka_received_at", current_timestamp())
    )

    query = (
        parsed.writeStream
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, spark))
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .start()
    )
    LOG.info("Spark Structured Streaming started")
    query.awaitTermination()


if __name__ == "__main__":
    main()
