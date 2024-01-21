from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("LinearRegressionExample").getOrCreate()

data = [(1.0, 2.0, 3.0), (2.0, 3.0, 4.0), (3.0, 4.0, 5.0), (4.0, 5.0, 6.0), (5.0, 6.0, 7.0)]

schema = ["Feature1", "Feature2", "Label"]

df = spark.createDataFrame(data, schema=schema)

df.show()

feature_cols = ["Feature1", "Feature2"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_features = assembler.transform(df)

lr = LinearRegression(featuresCol="features", labelCol="Label")
model = lr.fit(df_features)

print(f"Coefficients: {model.coefficients}")
print(f"Intercept: {model.intercept}")

spark.stop()
