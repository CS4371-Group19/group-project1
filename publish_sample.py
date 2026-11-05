"""Publish example records from a JSONL file to Kafka topic1."""

import argparse
import json
from pathlib import Path

from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="topic1")
    parser.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    try:
        with args.input.open(encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                record = json.loads(line)
                if not record.get("id") or not record.get("text", "").strip():
                    raise ValueError("Each record needs a nonempty id and text")
                producer.send(args.topic, key=record["id"], value=record).get(timeout=15)
                print(f"Published {record['id']}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
