from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName("SimplePySparkExample").getOrCreate()

data = [(1, 0.5, 0), (2, 1.0, 1), (3, 0.8, 0), (4, 1.2, 1), (5, 0.6, 0)]

schema = ["ID", "Feature1", "Label"]

df = spark.createDataFrame(data, schema=schema)

df.show()

feature_cols = ["Feature1"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_features = assembler.transform(df)

train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=42)

lr = LogisticRegression(featuresCol="features", labelCol="Label")
model = lr.fit(train_data)

predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator(labelCol="Label")
accuracy = evaluator.evaluate(predictions)

print(f"Accuracy: {accuracy}")

spark.stop()
