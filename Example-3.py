from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Create a Spark session
spark = SparkSession.builder.appName("RandomForestExample").getOrCreate()

# Load the Iris dataset
from sklearn.datasets import load_iris
import pandas as pd

iris = load_iris()
iris_df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
iris_df["label"] = iris.target

# Create a PySpark DataFrame from the Pandas DataFrame
df = spark.createDataFrame(iris_df)

df.show()

feature_cols = iris.feature_names
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_features = assembler.transform(df)

# Machine Learning - Random Forest Classifier
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=10)
model = rf.fit(df_features)

# Display feature importance
print("Feature Importance:")
for i, importance in enumerate(model.featureImportances.toArray()):
    print(f"{feature_cols[i]}: {importance}")

# Model Evaluation
predictions = model.transform(df_features)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print(f"Accuracy: {accuracy}")

spark.stop()
