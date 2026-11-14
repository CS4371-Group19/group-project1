import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand


parser = argparse.ArgumentParser(description="Select 50 messages for manual labeling")
parser.add_argument(
    "--input",
    default="output/classified_news",
    help="Path to the batch-classification Parquet output",
)
parser.add_argument(
    "--output",
    default="output/manual_label_sample",
    help="Directory where the manual-label CSV should be written",
)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("CS4371-Part2-3b-ManualLabelSample")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(args.input)

sample = (
    df.orderBy(rand(seed=42))
    .limit(50)
    .select(
        "id",
        "source",
        "text",
        "predicted_class",
        "predicted_probability"
    )
)

sample.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .option("multiLine", True) \
    .option("quoteAll", True) \
    .option("escape", '"') \
    .csv(args.output)

print("Selected 50 articles.")
print("Saved with CSV quoting enabled.")

spark.stop()
