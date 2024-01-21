from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import matplotlib.pyplot as plt
import seaborn as sns

spark = SparkSession.builder.appName("WeatherAnalysis").getOrCreate()

data = [
    ("2022-01-01", 25.0, 60, 0.1),
    ("2022-01-02", 26.5, 55, 0.0),
    ("2022-01-03", 24.8, 70, 0.2),
    # ... more data ...
]

columns = ["Date", "TemperatureCelsius", "HumidityPercent", "PrecipitationInches"]

df = spark.createDataFrame(data, columns)


df.show()

avg_temp = df.agg(avg("TemperatureCelsius").alias("AverageTemperature")).collect()[0]["AverageTemperature"]
avg_humidity = df.agg(avg("HumidityPercent").alias("AverageHumidity")).collect()[0]["AverageHumidity"]
total_precipitation = df.agg(avg("PrecipitationInches").alias("TotalPrecipitation")).collect()[0]["TotalPrecipitation"]

print(f"Average Temperature: {avg_temp:.2f}°C")
print(f"Average Humidity: {avg_humidity:.2f}%")
print(f"Total Precipitation: {total_precipitation:.2f} inches")

# Visualize the results using Seaborn and Matplotlib
sns.set(style="whitegrid")
plt.figure(figsize=(12, 6))

sns.lineplot(x="Date", y="TemperatureCelsius", data=df.toPandas(), label="Temperature (°C)", marker="o")
plt.title("Daily Temperature Over Time")
plt.xlabel("Date")
plt.ylabel("Temperature (°C)")
plt.xticks(rotation=45)
plt.legend()
plt.show()

plt.figure(figsize=(8, 6))
sns.scatterplot(x="HumidityPercent", y="PrecipitationInches", data=df.toPandas(), color="blue")
plt.title("Humidity vs. Precipitation")
plt.xlabel("Humidity (%)")
plt.ylabel("Precipitation (inches)")
plt.show()

spark.stop()
