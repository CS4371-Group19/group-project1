import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, when


parser = argparse.ArgumentParser(description="Evaluate 50 manually labeled messages")
parser.add_argument(
    "--input",
    default="output/manual_label_sample/manual_labels.csv",
    help="Path to the completed manual-label CSV",
)
parser.add_argument(
    "--output",
    default="output/manual_label_evaluation",
    help="Directory where the evaluation summary should be written",
)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("CS4371-Part2-3b-ManualEvaluation")
    .master("local[*]")
    .getOrCreate()
)

# Read the manually labeled sample. multiline/quote/escape preserve article text.
df = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .csv(args.input)
)

required = {"manual_label", "predicted_class"}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {sorted(missing)}")

df = df.withColumn("manual_label", col("manual_label").cast("string"))
df = df.withColumn("predicted_class", col("predicted_class").cast("string"))

total = df.count()
none_df = df.filter(col("manual_label") == "None")
eligible = df.filter(col("manual_label").isin("World", "Sports", "Business", "Sci/Tech"))

none_count = none_df.count()
eligible_count = eligible.count()
correct_count = eligible.filter(col("manual_label") == col("predicted_class")).count()
accuracy = correct_count / eligible_count if eligible_count else 0.0

print("\\n========== PART 2.3(b) MANUAL EVALUATION ==========")
print(f"Total manually labeled messages: {total}")
print(f"Messages labeled None: {none_count}")
print(f"Messages in the four AG News classes: {eligible_count}")
print(f"Correct predictions: {correct_count}")
print(f"Accuracy: {correct_count}/{eligible_count} = {accuracy:.4%}")

print("\\nManual-label counts:")
df.groupBy("manual_label").count().orderBy("manual_label").show()

print("\\nAccuracy by manual class:")
(
    eligible.groupBy("manual_label")
    .agg(
        count("*").alias("total"),
        spark_sum(when(col("manual_label") == col("predicted_class"), 1).otherwise(0)).alias("correct")
    )
    .withColumn("accuracy", col("correct") / col("total"))
    .orderBy("manual_label")
    .show(truncate=False)
)

print("\\nConfusion matrix (rows = manual labels, columns = model predictions):")
eligible.groupBy("manual_label", "predicted_class").count().orderBy("manual_label", "predicted_class").show(100, truncate=False)

# Save a compact, reproducible results file.
summary = spark.createDataFrame(
    [
        ("total_messages", total),
        ("none_messages", none_count),
        ("eligible_messages", eligible_count),
        ("correct_predictions", correct_count),
    ],
    ["metric", "value"]
)
summary.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output)

spark.stop()
