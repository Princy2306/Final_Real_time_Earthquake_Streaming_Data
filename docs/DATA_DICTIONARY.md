# Data Dictionary

## Normalized event record

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Unique USGS event identifier |
| `magnitude` | double | Reported earthquake magnitude |
| `place` | string | USGS human-readable location |
| `time` | long | Event origin time, Unix epoch milliseconds |
| `longitude` | double | Longitude in decimal degrees |
| `latitude` | double | Latitude in decimal degrees |
| `depth` | double | Depth in kilometres |
| `event_type` | string | USGS event type |
| `status` | string | USGS review status |
| `url` | string | USGS event page |
| `source_updated` | long | Last USGS update time, epoch milliseconds |
| `ingested_at` | string | UTC ingestion timestamp |
| `source_feed` | string | Feed URL used by the poller |

## Delta Lake tables

- `bronze/earthquakes`: structured event payloads received from Kafka plus ingestion batch metadata.
- `silver/earthquakes`: validated, deduplicated events with `event_time`, `event_date`, `is_unusual_magnitude`, and `processed_at`.
- `gold/clusters`: candidate cluster-level summaries.
- `gold/cluster_events`: event membership for each candidate cluster.

`is_unusual_magnitude` is a configurable quality flag, not a claim that a magnitude is physically impossible.
