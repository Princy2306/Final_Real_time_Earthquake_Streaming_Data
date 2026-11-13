#!/usr/bin/env bash
set -euo pipefail
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic earthquakes --partitions 3 --replication-factor 1
