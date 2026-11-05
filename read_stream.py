"""Smoke test: read JSON records from Kafka with Spark Structured Streaming."""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType


SCHEMA = StructType(
    [StructField(name, StringType()) for name in
     ("id", "source", "source_type", "text", "published_at", "ingested_at")]
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="topic1")
    parser.add_argument("--checkpoint", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("gp1-kafka-smoke-test").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    messages = (
        raw.select(from_json(col("value").cast("string"), SCHEMA).alias("message"))
        .select("message.*")
        .where(col("id").isNotNull() & col("text").isNotNull())
    )
    query = (
        messages.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", args.checkpoint)
        .start()
    )
    try:
        query.awaitTermination()
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
