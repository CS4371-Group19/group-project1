import argparse

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel


parser = argparse.ArgumentParser(description="Inspect a saved AG News PipelineModel")
parser.add_argument(
    "--model",
    default="models/ag_news_lr_model",
    help="Path to the saved PipelineModel",
)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("InspectAGNewsModel")
    .master("local[*]")
    .getOrCreate()
)

print("\nLoading saved model...")

model = PipelineModel.load(args.model)

print("\nMODEL LOADED SUCCESSFULLY")

for stage in model.stages:
    print(type(stage).__name__)

lr = model.stages[-1]

print("\nNumber of classes:", lr.numClasses)
print("Prediction column:", lr.getPredictionCol())
print("Probability column:", lr.getProbabilityCol())

spark.stop()
