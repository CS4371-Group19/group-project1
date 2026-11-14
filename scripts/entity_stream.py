"""Smoke test: read JSON records from Kafka with Spark Structured Streaming."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, udf, explode, struct, to_json, when
)
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.ml import PipelineModel
import spacy
import os
from dotenv import load_dotenv

load_dotenv() 

SCHEMA = StructType([
    StructField(name, StringType()) 
    for name in ("id", "source", "text", "published_at", "ingested_at", "url")
])
nlp = spacy.load("en_core_web_sm")


@udf(returnType=ArrayType(StringType()))
def extract_entities_udf(text: str):
    """Extract entities from the text"""
    doc = nlp(text)
    return [ent.text for ent in doc.ents]


if __name__ == "__main__":
    if os.environ["KAFKA_BOOTSTRAP_SERVERS"] == "":
        raise Exception("Missing KAFKA_BOOTSTRAP_SERVERS")
    if os.environ["ARTICLE_TOPIC"] == "":
        raise Exception("Missing ARTICLE_TOPIC")
    if os.environ["ENTITY_COUNT_TOPIC"] == "":
        raise Exception("Missing ENTITY_COUNT_TOPIC")
    if os.environ["CLASSIFICATION_COUNT_TOPIC"] == "":
        raise Exception("Missing CLASSIFICATION_COUNT_TOPIC")

    kafka_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    article_topic = os.environ["ARTICLE_TOPIC"]
    entity_count_topic = os.environ["ENTITY_COUNT_TOPIC"]
    classification_count_topic = os.environ["CLASSIFICATION_COUNT_TOPIC"]

    spark = SparkSession.builder.appName("entity-and-classification-counts").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    model = PipelineModel.load("/app/models/ag_news_lr_model")

    # Read the data stream from kafka
    json_articles = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", article_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse and filter out the invalid data
    articles = (
        json_articles
        .select(from_json(col("value").cast("string"), SCHEMA).alias("message"))
        .select("message.*")
        .where(col("id").isNotNull() & col("text").isNotNull())
    )

    # Apply the saved AG News classification model
    classified_articles = (
        model.transform(articles)
        .withColumn(
            "predicted_class",
            when(col("prediction") == 0, "World")
            .when(col("prediction") == 1, "Sports")
            .when(col("prediction") == 2, "Business")
            .when(col("prediction") == 3, "Sci/Tech")
        )
    )

    # Maintain running counts for each predicted class
    classification_counts = (
        classified_articles
        .groupBy("predicted_class")
        .count()
    )

    def publish_classification_batch(batch_df, batch_id):
        (
            batch_df
            .select(
                col("predicted_class").alias("key"),
                to_json(
                    struct("predicted_class", "count")
                ).alias("value")
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_servers)
            .option("topic", classification_count_topic)
            .save()
        )



    # Extract the entities from text and keep count
    entity_counts = (
        articles
        .withColumn("entities", extract_entities_udf(col("text")))
        .select(explode(col("entities")).alias("entity"))
        .groupBy("entity")
        .count()
    )

    publish_kafka = (
        entity_counts
        # Kafka writer expects DataFrame with columns named key and value
        .select(col("entity").alias("key"), to_json(struct("entity", "count")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("topic", entity_count_topic)
        .option("checkpointLocation", "checkpoints/count_entities")
        .outputMode("update")
        .start()    
    )

    publish_classifications = (
        classification_counts
        .writeStream
        .foreachBatch(publish_classification_batch)
        .outputMode("update")
        .option("checkpointLocation", "checkpoints/classify_articles")
        .start()
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        publish_kafka.stop()
        publish_classifications.stop()
        spark.stop()
