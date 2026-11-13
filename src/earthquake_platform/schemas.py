from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, MapType
)

EARTHQUAKE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("magnitude", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", LongType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("depth", DoubleType(), True),
    StructField("event_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("url", StringType(), True),
    StructField("source_updated", LongType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("source_feed", StringType(), True),
])
