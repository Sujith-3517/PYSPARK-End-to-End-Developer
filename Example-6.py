import pandas as pd
from sklearn.datasets import load_iris

# Load the Iris dataset
iris = load_iris()
iris_df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
iris_df['target'] = iris.target

# Display the first few rows of the dataset
print("First few rows of the dataset:")
print(iris_df.head())

# Basic statistical analysis
print("\nStatistical summary of the dataset:")
print(iris_df.describe())

# Filter the dataset based on a condition
setosa_species = iris_df[iris_df['target'] == 0]
print("\nSubset of the dataset with Setosa species:")
print(setosa_species)

# Group the dataset by target and calculate the mean for each group
grouped_means = iris_df.groupby('target').mean()
print("\nGrouped mean values for each target:")
print(grouped_means)

# Correlation matrix
correlation_matrix = iris_df.corr()
print("\nCorrelation matrix:")
print(correlation_matrix)

# Plotting a histogram
import matplotlib.pyplot as plt

iris_df.hist(bins=20, figsize=(12, 8))
plt.suptitle("Histograms of Iris Dataset Features", y=1.02, size=16)
plt.show()
