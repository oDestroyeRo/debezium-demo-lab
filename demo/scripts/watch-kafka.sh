#!/bin/sh
set -eu

topic="${1:-orders.events.v1}"

docker compose exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic "${topic}" \
  --from-beginning \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.headers=true
