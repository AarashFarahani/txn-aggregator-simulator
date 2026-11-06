# Transaction Processing Platform

Local demo: React simulator → Kafka → Spark Structured Streaming (2 workers) → Kafka → Spring WebFlux/Reactor Kafka → PostgreSQL R2DBC → Grafana.

## Run
Requirements: Docker Engine and Docker Compose v2. Recommended host memory: 8 GB+.

```bash
docker compose up -d --build
docker compose ps
```

- Simulator: http://localhost:3000
- Simulator API: http://localhost:8080
- Grafana: http://localhost:3001 (admin / admin)
- Spark Master: http://localhost:8081
- Spark Worker UIs: http://localhost:8082 and http://localhost:8083

In the simulator UI, use the default 1,000,000 transactions, batch size 1,000, concurrency 4, then Start.

## Topics and processing
- `transactions.raw`: one JSON transaction per Kafka message.
- `transactions.aggregated`: per-minute aggregate records.
- Spark computes COUNTRY, MERCHANT, and TRANSACTION_TYPE aggregates with a 1-minute tumbling event-time window and 10-second watermark.
- Spark checkpoint state is stored in the `spark-checkpoints` Docker volume.
- The reactive consumer commits offsets only after PostgreSQL upsert succeeds.
- PostgreSQL uses a unique aggregation ID and `ON CONFLICT` to make redelivery safe.

## Operations
```bash
docker compose logs -f spark-job
docker compose logs -f transaction-consumer
docker compose down                 # retain volumes
docker compose down -v              # DELETE all persisted data
```

This is a local development topology: one Kafka broker (replication factor 1), two Spark worker containers on one host. It is not HA. Tune worker memory/cores and Kafka partitions to suit your machine. Spark emits a window after the watermark passes its end, so dashboards update slightly after each minute boundary.
