"""Poll the USGS GeoJSON feed and publish newly observed events to Kafka."""
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

LOG = logging.getLogger("earthquake-ingestion")

USGS_FEED_URL = os.getenv(
    "USGS_FEED_URL",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes")
CHECKPOINT_DB = os.getenv("INGESTION_CHECKPOINT_DB", "/app/data/seen_events.sqlite3")


def normalize_feature(feature):
    """Convert a USGS GeoJSON Feature to the project's stable event schema."""
    props = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}
    coords = geometry.get("coordinates") or []
    if len(coords) < 3:
        coords = [None, None, None]
    return {
        "id": feature.get("id"),
        "magnitude": props.get("mag"),
        "place": props.get("place"),
        "time": props.get("time"),
        "longitude": coords[0],
        "latitude": coords[1],
        "depth": coords[2],
        "event_type": props.get("type"),
        "status": props.get("status"),
        "url": props.get("url"),
        "source_updated": props.get("updated"),
        "raw_properties": props,
        "raw_geometry": geometry,
    }


def init_checkpoint(path=CHECKPOINT_DB):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE IF NOT EXISTS seen_events (event_id TEXT PRIMARY KEY, first_seen_utc TEXT)")
    conn.commit()
    return conn


def is_new_event(conn, event_id):
    if not event_id:
        return False
    try:
        conn.execute(
            "INSERT INTO seen_events(event_id, first_seen_utc) VALUES (?, ?)",
            (event_id, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS.split(","),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
    )


def poll_once(session, producer, checkpoint):
    response = session.get(USGS_FEED_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    sent = 0

    for feature in payload.get("features", []):
        event = normalize_feature(feature)
        if not event["id"] or not is_new_event(checkpoint, event["id"]):
            continue
        event["ingested_at"] = datetime.now(timezone.utc).isoformat()
        event["source_feed"] = USGS_FEED_URL
        future = producer.send(KAFKA_TOPIC, value=event)
        try:
            future.get(timeout=30)
            sent += 1
        except KafkaError:
            LOG.exception("Kafka publish failed for event %s", event["id"])
            # Keep checkpoint semantics simple for this demo: a failed send is logged.
            # On restart, clear that event ID from SQLite to permit retry.
            checkpoint.execute("DELETE FROM seen_events WHERE event_id = ?", (event["id"],))
            checkpoint.commit()
    producer.flush()
    LOG.info("Feed returned %d records; published %d new events", len(payload.get("features", [])), sent)
    return sent


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    while True:
        try:
            producer = create_producer()
            checkpoint = init_checkpoint()
            with requests.Session() as session:
                LOG.info("Starting USGS -> Kafka poller: %s", USGS_FEED_URL)
                while True:
                    try:
                        poll_once(session, producer, checkpoint)
                    except requests.RequestException:
                        LOG.exception("USGS request failed; will retry")
                    except Exception:
                        LOG.exception("Unexpected ingestion error")
                    time.sleep(POLL_INTERVAL)
        except Exception:
            LOG.exception("Unable to initialize ingestion dependencies; retrying in 10 seconds")
            time.sleep(10)


if __name__ == "__main__":
    main()
