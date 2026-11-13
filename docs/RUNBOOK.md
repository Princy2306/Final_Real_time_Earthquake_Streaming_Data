# Runbook

## Verify Kafka
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic earthquakes --from-beginning
```

## Verify MinIO
Open `http://localhost:9001`, sign in with the values in `.env`, and check the `earthquake-data` bucket.

## Restart one component
```bash
docker compose restart ingestion
docker compose restart spark-processor
```

## Reset local state
This removes Kafka/MinIO/ingestion volumes and all locally persisted data:
```bash
docker compose down -v
```

## Common issues
- **Kafka connection refused:** wait for the Kafka healthcheck; inspect `docker compose logs kafka`.
- **No new events:** the all-day feed only returns events currently present in that feed. The poller publishes only IDs not already in its SQLite checkpoint.
- **No Gold table yet:** the configured candidate-cluster minimum may not be met by recent events. Check Silver first in MinIO.
- **Spark package download failure:** ensure the Docker host can reach Maven Central when the Spark processor first starts.
- **MinIO credentials mismatch:** keep `.env` credentials aligned with the values passed to the Spark and dashboard services.
