import argparse
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

# Schema matching the messages published to topic1 by news_producer.py
TOPIC1_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("source", StringType()),
    StructField("text", StringType()),
    StructField("published_at", StringType()),
    StructField("ingested_at", StringType()),
    StructField("url", StringType()),
])

LABEL_MAPPING = {
    0: "World",
    1: "Sports",
    2: "Business",
    3: "Sci/Tech"
}

def parse_args():
    parser = argparse.ArgumentParser(description="CS4371 Part 2.1 & 2.2: Data Loading & Model Training")
    parser.add_argument(
        "--ag-news-path",
        type=str,
        default=os.environ.get("AG_NEWS_PATH", "data/ag_news/train.parquet"),
        help="Path to AG News train.parquet"
    )
    parser.add_argument(
        "--model-output-path",
        type=str,
        default=os.environ.get("MODEL_OUTPUT_PATH", "models/ag_news_lr_model"),
        help="Path where the fitted PipelineModel should be saved"
    )
    parser.add_argument(
        "--kafka-servers",
        type=str,
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=os.environ.get("ARTICLE_TOPIC", "topic1"),
        help="Kafka topic containing streamed messages"
    )
    parser.add_argument(
        "--skip-kafka-check",
        action="store_true",
        help="Skip Kafka reading if only fitting the model offline"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
            SparkSession.builder
            .appName("CS4371-Part2-TrainAndLoad")
            .master("local[*]")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
    spark.sparkContext.setLogLevel("WARN")

    print("PART 2.1: Loading Data")

    # Load AG News training parquet
    print(f"Loading AG News data from: {args.ag_news_path}")
    ag_news_df = spark.read.parquet(args.ag_news_path)

    # Report class distribution
    print("\nAG News Class Counts:")
    counts_df = ag_news_df.groupBy("label").count().orderBy("label")
    counts_df.show()

    # Formatting
    counts_collected = counts_df.collect()
    for row in counts_collected:
        lbl = row["label"]
        cnt = row["count"]
        class_name = LABEL_MAPPING.get(lbl, f"Unknown ({lbl})")
        print(f"  Class {lbl} ({class_name}): {cnt:,} articles")

    total_ag_articles = ag_news_df.count()
    print(f"Total AG News training articles: {total_ag_articles:,}\n")

    # Read streamed messages from Kafka (Batch read from earliest offset)
    if not args.skip_kafka_check:
        print(f"Reading batch messages from Kafka topic: '{args.topic}' via {args.kafka_servers}")
        try:
            raw_kafka_df = (
                spark.read
                .format("kafka")
                .option("kafka.bootstrap.servers", args.kafka_servers)
                .option("subscribe", args.topic)
                .option("startingOffsets", "earliest")
                .load()
            )

            # Parse JSON value and extract text column
            parsed_df = (
                raw_kafka_df
                .select(from_json(col("value").cast("string"), TOPIC1_SCHEMA).alias("msg"))
                .select("msg.text")
            )

            total_raw_msgs = parsed_df.count()
            print(f"Total raw messages ingested from Kafka: {total_raw_msgs:,}")

            # Filter empty / null and drop duplicates
            cleaned_msgs_df = (
                parsed_df
                .filter(col("text").isNotNull())
                .filter(col("text") != "")
                .filter(col("text").rlike(r"\S"))
                .dropDuplicates(["text"])
            )

            remaining_msgs = cleaned_msgs_df.count()
            print(f"Messages remaining after dropping duplicates/empties: {remaining_msgs:,}")

            if remaining_msgs < 1000:
                print(
                    f"\n[WARNING] Found {remaining_msgs} unique messages. "
                    "The prompt requires at least 1,000 streamed messages. Make sure your producer "
                    "has collected enough articles before final submission.\n"
                )
            else:
                print(f"[OK] Streamed messages threshold met (>= 1,000).")

        except Exception as e:
            print(f"[WARNING] Could not read from Kafka: {e}")
            print("If Kafka is running in Docker, ensure ports are exposed or run inside Compose.")

    print("\n==================================================")
    print("PART 2.2: BUILDING THE PIPELINE")
    print("==================================================\n")

    # Pipeline stages:
    # RegexTokenizer: lowercase and strip punctuation (keeps only word characters \w+)
    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="tokens",
        pattern=r"\W+",
        toLowercase=True
    )

    # StopWordsRemover: drop english stopwords
    remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="filtered_tokens"
    )

    # TF-IDF using HashingTF + IDF
    # numFeatures=20000 provides high vocabulary coverage without excessive memory usage
    hashing_tf = HashingTF(
        inputCol="filtered_tokens",
        outputCol="raw_features",
        numFeatures=20000
    )

    idf = IDF(
        inputCol="raw_features",
        outputCol="features",
        minDocFreq=2
    )

    # LogisticRegression classifier
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=20,
        regParam=0.01
    )

    pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, lr])

    print("Fitting MLlib Pipeline on AG News training data (this may take 1-2 minutes)...")
    model = pipeline.fit(ag_news_df)
    print("Pipeline fitting complete!")

    # Save fitted model
    print(f"Saving fitted PipelineModel to: {args.model_output_path}")
    model.write().overwrite().save(args.model_output_path)
    print(f"Model successfully saved to {args.model_output_path}!\n")

    spark.stop()


if __name__ == "__main__":
    main()