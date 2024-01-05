from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()

people_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///path/to/spark-3.1.2-bin-hadoop3.2/examples/src/main/resources/people.csv")

people_df.createOrReplaceTempView("people_table")

all_columns_query = spark.sql("SELECT * FROM people_table")

specific_columns_query = spark.sql("SELECT name, age FROM people_table")

age_filter_query = spark.sql("SELECT * FROM people_table WHERE age >= 30")

group_by_query = spark.sql("SELECT age, COUNT(*) as count FROM people_table GROUP BY age ORDER BY age")

print("All Columns Query:")
all_columns_query.show()

print("Specific Columns Query:")
specific_columns_query.show()

print("Age Filter Query:")
age_filter_query.show()

print("Group By Query:")
group_by_query.show()

spark.stop()
