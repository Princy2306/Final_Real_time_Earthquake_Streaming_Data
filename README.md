# Real-Time Earthquake & Aftershock Detection Platform

A cloud-native, streaming data engineering project that ingests USGS earthquake events, publishes them to Apache Kafka, processes them with Spark Structured Streaming, stores data in a MinIO + Delta Lake lakehouse, and derives **potential** mainshock–aftershock clusters for historical analysis.

> **Scientific limitation:** the cluster detector in this repository is a configurable heuristic for academic demonstration. It is not an official USGS sequence classifier, earthquake forecast, or early-warning system.

## Architecture

```text
USGS GeoJSON feed
       |
       v
Python ingestion service (poll + event-ID deduplication)
       |
       v
Kafka topic: earthquakes
       |
       v
Spark Structured Streaming
       |
       +----> Bronze Delta table (raw events + ingestion metadata)
       |
       v
Cleaning / validation / standardization
       |
       v
Silver Delta table
       |
       v
Spatiotemporal correlation (potential clusters)
       |
       v
Gold Delta tables (cluster summaries + event membership)
       |
       v
Streamlit analytics dashboard
```

## Technology choices

| Layer | Technology |
|---|---|
| Source | USGS GeoJSON feeds |
| Ingestion | Python, Requests |
| Messaging | Apache Kafka 4.1.2 |
| Processing | Apache Spark Structured Streaming 4.0.x |
| Lakehouse tables | Delta Lake 4.0.x |
| Object storage | MinIO (S3-compatible) |
| Local orchestration | Docker Compose |
| Optional orchestration | Kubernetes manifests |
| Dashboard | Streamlit, Plotly |

The stack is pinned to Spark 4.0.x / Delta Lake 4.0.x. Kafka 4.1.2 uses KRaft and does not require ZooKeeper.

## Repository layout

```text
.
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── config/
│   └── settings.yaml
├── src/earthquake_platform/
│   ├── ingestion.py
│   ├── spark_stream.py
│   ├── clustering.py
│   └── schemas.py
├── dashboard/app.py
├── scripts/create_topic.sh
├── k8s/
├── tests/
└── notebooks/DataRetrieval.ipynb
```

## Quick start (Docker Compose)

Prerequisites: Docker Desktop / Docker Engine with Compose plugin, and at least 8 GB RAM recommended.

1. Copy environment settings:

   ```bash
   cp .env.example .env
   ```

2. Start Kafka and MinIO:

   ```bash
   docker compose up -d kafka minio minio-init
   ```

3. Start ingestion and Spark processing:

   ```bash
   docker compose up -d ingestion spark-processor
   ```

4. Open MinIO Console at `http://localhost:9001`.
   - Default local credentials are in `.env`.
   - The initialization service creates the `earthquake-data` bucket and Delta prefixes.

5. Check logs:

   ```bash
   docker compose logs -f ingestion
   docker compose logs -f spark-processor
   ```

6. Start the dashboard locally:

   ```bash
   python -m venv .venv
   source .venv/bin/activate        # Windows: .venv\Scripts\activate
   pip install -r requirements-dashboard.txt
   streamlit run dashboard/app.py
   ```

   The dashboard reads Gold output from the MinIO bucket. Set the same MinIO environment variables as in `.env`.

Stop the stack:

```bash
docker compose down
```

To remove local data too (destructive):

```bash
docker compose down -v
```

## Data layers

- **Bronze:** original event payload, USGS event ID, ingestion timestamp, source feed.
- **Silver:** standardized event timestamp, coordinates, depth and magnitude; invalid rows are filtered and unusual magnitudes are flagged rather than silently deleted.
- **Gold:** potential cluster summaries and event-to-cluster membership, including event count, mainshock magnitude, spatial extent and activity time range.

All table locations are under `s3a://earthquake-data/lakehouse/`.

## Aftershock correlation heuristic

The initial detector uses configurable parameters in `config/settings.yaml`:

- maximum time difference from a candidate mainshock;
- maximum epicentral distance;
- minimum number of events in a cluster.

For each connected spatiotemporal group, the largest-magnitude event is treated as the candidate mainshock. Smaller events occurring after it are marked as potential aftershocks. This is a screening heuristic, not a seismological model. The thresholds should be calibrated and validated against authoritative sequence labels before making scientific claims.

## Configuration

Edit `config/settings.yaml` or environment variables:

- `USGS_FEED_URL`
- `POLL_INTERVAL_SECONDS`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET`

Never commit real credentials. `.env` is ignored by Git.

## Development and tests

```bash
pip install -r requirements.txt
pytest -q
```

## Current implementation notes

- The original course notebook is retained in `notebooks/` as development evidence.
- The services in this repository are a cleaned, modular implementation of the same pipeline.
- The ingestion service keeps a local SQLite event-ID checkpoint so restarts do not immediately republish every event seen in the previous process.
- Kafka delivery is at-least-once; downstream Delta writes use event IDs for idempotent deduplication.
- Spark writes Bronze and Silver Delta tables and refreshes Gold cluster outputs per micro-batch.
- Kubernetes YAML is a starting deployment scaffold. Resource limits, secrets, persistent volumes, probes and production security settings must be reviewed for your cluster.

## Team

Group 20 — Real-Time Earthquake & Aftershock Detection Using Streaming Data.
