from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# Create a Spark session
spark = SparkSession.builder.appName("PySparkMLExample").getOrCreate()

# Load the Iris dataset (you can replace it with your own dataset)
# For this example, I'm using the built-in Iris dataset available in PySpark
iris_df = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

# Display the first few rows of the dataset
iris_df.show(5)

# Prepare the features by assembling them into a single vector
feature_columns = iris_df.columns[:-1]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
assembled_data = assembler.transform(iris_df)

# Split the data into training and testing sets
(training_data, testing_data) = assembled_data.randomSplit([0.8, 0.2], seed=42)

# Create a Logistic Regression model
lr = LogisticRegression(featuresCol="features", labelCol="label")

# Create a pipeline with the assembler and the logistic regression model
pipeline = Pipeline(stages=[assembler, lr])

# Train the model
model = pipeline.fit(training_data)

# Make predictions on the testing data
predictions = model.transform(testing_data)

# Display the predictions
predictions.select("label", "prediction", "probability").show(5)

# Evaluate the model
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
area_under_roc = evaluator.evaluate(predictions)
print(f"Area under ROC curve: {area_under_roc}")

# Stop the Spark session
spark.stop()
