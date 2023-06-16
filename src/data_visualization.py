import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.decomposition import PCA
from pyspark.sql import SparkSession

from distance_functions import route_distance
from clustering import run_clustering
import matplotlib.pyplot as plt

# from parse_data import parse_json_data, encode_data

def convert_pd_df_to_one_row(pd_df):
    # Create spark session
    spark = SparkSession.builder.appName("Clustering").getOrCreate()
    # Encode each route as one row using spark
    encoded_spark_df, product_list = encode_data(spark, pd_df, False) # yo man, dit geeft errors omdat ie probeert data te lezen uit path die niet precies klopt, kijk ff of je dit naar de meest recente versie kunt updated
    # Convert back to pandas for processing
    encoded_pd_df = encoded_spark_df.toPandas()
    return encoded_pd_df

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
