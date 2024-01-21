from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
import matplotlib.pyplot as plt
import seaborn as sns

spark = SparkSession.builder.appName("EcommerceAnalysis").getOrCreate()

# Simulate a hypothetical e-commerce dataset
data = [
    (1, "John", "Electronics", 100.0, "2022-01-01"),
    (2, "Jane", "Clothing", 50.0, "2022-01-01"),
    (3, "Bob", "Electronics", 120.0, "2022-01-02"),
    # ... more data ...
]

columns = ["CustomerID", "CustomerName", "Category", "AmountSpent", "PurchaseDate"]

df = spark.createDataFrame(data, columns)

df.show()

category_counts = df.groupBy("Category").agg(count("*").alias("TotalPurchases"), avg("AmountSpent").alias("AvgAmountSpent"))
category_counts.show()

# Visualize the results using Seaborn and Matplotlib
sns.set(style="whitegrid")
plt.figure(figsize=(10, 6))

sns.barplot(x="Category", y="TotalPurchases", data=category_counts.toPandas(), palette="viridis")
plt.title("Total Purchases per Category")
plt.xlabel("Category")
plt.ylabel("Total Purchases")
plt.show()

plt.figure(figsize=(10, 6))
sns.boxplot(x="Category", y="AvgAmountSpent", data=category_counts.toPandas(), palette="magma")
plt.title("Average Amount Spent per Category")
plt.xlabel("Category")
plt.ylabel("Average Amount Spent")
plt.show()

spark.stop()
