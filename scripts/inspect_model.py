from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

spark = (
    SparkSession.builder
    .appName("InspectAGNewsModel")
    .master("local[*]")
    .getOrCreate()
)

print("\nLoading saved model...")

model = PipelineModel.load("/app/models/ag_news_lr_model")

print("\nMODEL LOADED SUCCESSFULLY")

for stage in model.stages:
    print(type(stage).__name__)

lr = model.stages[-1]

print("\nNumber of classes:", lr.numClasses)
print("Prediction column:", lr.getPredictionCol())
print("Probability column:", lr.getProbabilityCol())

spark.stop()