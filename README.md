# CS4371 Group Project 1 starter

First integration milestone: publish two sample JSON messages to Kafka `topic1` and read them in PySpark Structured Streaming. This runs Spark in local mode. Add the Hacker News producer and entity processing after this path works.

## Prerequisites

- Docker with Compose
- Python 3.10+ (Spark 4.0.1 supports Python 3.9+)
- Java 17 or 21 on the host (`java -version`)

## Start Kafka and create topics

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic topic1 --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic topic2 --partitions 1 --replication-factor 1
```

Inside Compose, clients use `kafka:19092`. Python and Spark launched on your laptop use `localhost:9092`. If port 9092 is occupied, update the mapped port, advertised host listener, and command arguments together.

## Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

On Windows, use WSL or activate the virtual environment with your shell's equivalent command.

## Smoke test

In terminal A, from the project directory:

```bash
spark-submit --master 'local[*]' --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 scripts/read_stream.py --bootstrap-servers localhost:9092 --topic topic1 --checkpoint checkpoints/smoke
```

The first invocation needs to resolve the Spark Kafka connector and its dependencies. In terminal B:

```bash
source .venv/bin/activate
python scripts/publish_sample.py --bootstrap-servers localhost:9092 --topic topic1 --input examples/topic1.jsonl
```

Terminal A should print two rows with the sample IDs and text. Press Ctrl+C to stop it. A checkpoint remembers offsets; use a different checkpoint path if you intentionally want to replay old messages. Do not commit checkpoint data.

## Topic contract

Each Kafka record key is its stable source ID, e.g. `hn:12345678`; its value is a UTF-8 JSON object with `id`, `source`, `source_type`, `text`, `published_at`, and `ingested_at`. Timestamps are UTC ISO 8601 strings. Keep the normalized text in `text` so entity processing and ML classification can share the same input. The producer should clean HTML, ignore empty/deleted items, and avoid republishing IDs across polls. Agree as a team whether `text` combines story title and body or also includes comments.

## Next work

1. Add a periodic Hacker News fetcher that persists seen item IDs and publishes normalized story/comment records to `topic1`.
2. Replace the console read with entity extraction and running counts, then emit `topic2` records.
3. Add the saved ML model as a second streaming query over `topic1` and the visualization stack.

The provided smoke test does not implement named entity counting or classification yet. Keep paths and broker addresses configurable when adding those parts. Never commit secrets.
