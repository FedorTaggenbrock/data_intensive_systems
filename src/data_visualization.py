import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.decomposition import PCA
from pyspark.sql import SparkSession
from parse_data import parse_json_data, encode_data
from distance_function import route_distance
from clustering import run_clustering
import matplotlib.pyplot as plt


def plot_routes(data:pd.DataFrame):
    le = LabelEncoder()
    data['from'] = le.fit_transform(data['from'])
    data['to'] = le.fit_transform(data['to'])

    # Normalize data
    scaler = StandardScaler()
    scaled_df = scaler.fit_transform(data)

    # Apply PCA
    pca = PCA(n_components=2)  # 2 for visualization in 2D space
    principal_components = pca.fit_transform(scaled_df)

    # Convert the principal components into a DataFrame
    pr_df = pd.DataFrame(data=principal_components, columns=['PC1', 'PC2'])

    plt.figure(figsize=(10, 8))
    plt.scatter(pr_df['PC1'], pr_df['PC2'])
    plt.title('Scatter plot of PC1 vs PC2')
    plt.xlabel('PC1')
    plt.ylabel('PC2')
    plt.show()


    print('')
    # Transform the data
    # df = pca.fit_transform(data)
    # print(df.shape)

def plot_results(results):
    pass

# if __name__ == '__main__':
#     plot_test()
