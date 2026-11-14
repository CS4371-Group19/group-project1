from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

spark = (
    SparkSession.builder
    .appName("CS4371-Part2-3b-ManualLabelSample")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet("/app/output/classified_news")

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
    .csv("/app/output/manual_label_sample")

print("Selected 50 articles.")
print("Saved with CSV quoting enabled.")

spark.stop()