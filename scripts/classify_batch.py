
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, rand, element_at
)
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array


LABEL_MAPPING = {
    0: "World",
    1: "Sports",
    2: "Business",
    3: "Sci/Tech"
}


def main():
    parser = argparse.ArgumentParser(
        description="CS4371 Project 2.3(a): Batch News Classification"
    )

    parser.add_argument(
        "--input",
        default="streamed_news.json",
        help="Path to streamed news JSONL file"
    )

    parser.add_argument(
        "--model",
        default="models/ag_news_lr_model",
        help="Path to saved AG News PipelineModel"
    )

    parser.add_argument(
        "--output",
        default="output/classified_news",
        help="Output directory for predictions"
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("CS4371-Part2-3a-BatchClassification")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\nLoading saved AG News PipelineModel...")
    model = PipelineModel.load(args.model)

    print("Model loaded successfully.")

    print(f"\nReading streamed messages from: {args.input}")

    news_df = spark.read.json(args.input)

    raw_count = news_df.count()
    print(f"Raw messages: {raw_count:,}")

    # Remove null, empty, and duplicate messages
    clean_df = (
        news_df
        .filter(col("text").isNotNull())
        .filter(col("text").rlike(r"\S"))
        .dropDuplicates(["text"])
    )

    clean_count = clean_df.count()

    print(f"Messages after cleaning: {clean_count:,}")
    print(f"Messages removed: {raw_count - clean_count:,}")

    # Apply the existing saved model
    predictions = model.transform(clean_df)

    # Convert probability vector into an array
    predictions = predictions.withColumn(
        "probability_array",
        vector_to_array(col("probability"))
    )

    # Extract probability associated with predicted class
    predictions = predictions.withColumn(
        "predicted_probability",
        element_at(
            col("probability_array"),
            col("prediction").cast("int") + 1
        )
    )

    # Convert numeric predictions into category names
    predictions = predictions.withColumn(
        "predicted_class",
        when(col("prediction") == 0, "World")
        .when(col("prediction") == 1, "Sports")
        .when(col("prediction") == 2, "Business")
        .when(col("prediction") == 3, "Sci/Tech")
        .otherwise("Unknown")
    )

    print("\nPREDICTED CLASS COUNTS")

    predictions.groupBy("predicted_class") \
        .count() \
        .orderBy("predicted_class") \
        .show(truncate=False)

    print("\nFIVE EXAMPLE ARTICLES PER CLASS")

    for class_name in LABEL_MAPPING.values():
        print(f"\n--- {class_name} ---")

        (
            predictions
            .filter(col("predicted_class") == class_name)
            .orderBy(rand())
            .select(
                "source",
                "text",
                "predicted_class",
                "predicted_probability"
            )
            .show(5, truncate=120)
        )

    # Save predictions for every cleaned message
    output_df = predictions.select(
        "id",
        "source",
        "text",
        "published_at",
        "url",
        "prediction",
        "predicted_class",
        "predicted_probability"
    )

    output_df.write.mode("overwrite").parquet(args.output)

    print(f"\nAll predictions saved to: {args.output}")
    print(f"Total classified messages: {clean_count:,}")

    spark.stop()


if __name__ == "__main__":
    main()
