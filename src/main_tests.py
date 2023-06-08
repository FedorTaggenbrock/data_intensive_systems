from pyspark.sql import SparkSession
from parse_data import parse_json_data, encode_data
from data_visualization import plot_routes
from clustering import run_clustering
from os import getcwd
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from distance_functions import test_distance_function
import matplotlib.pyplot as plt

def run_all_tests():
    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [2, 3],
        'max_iterations': 5,
        'debug_flag': True
    }

    #main function which runs all other tests imported from different files
    spark = SparkSession.builder.appName("Clustering").getOrCreate()
    print("Initialized Spark.")

    #Opletten dat bij het parsen de hoeveelheden van stad A-> stad B wel goed samengevoegd worden. Zie nu twee keer dezelfde from->to staan bij route 1 namelijk.
    pd_df, num_routes = parse_json_data()
    clustering_settings["num_routes"] = num_routes

    encoded_spark_df, product_list = encode_data(spark, pd_df, clustering_settings["debug_flag"])
    encoded_spark_rdd = encoded_spark_df.rdd

    if clustering_settings["debug_flag"]:
        test_distance_function(encoded_spark_rdd)

    print("Running run_clustering().")
    centroids = run_clustering(
        data=encoded_spark_rdd,
        clustering_settings=clustering_settings
        )
    print("The centroids are given by: ", centroids)

    # print("Start evaluating clusters")
    return

def plot_test():
    # Load data and create data frame
    pd_df, num_routes = parse_json_data()
    # Create spark session
    spark = SparkSession.builder.appName("Clustering").getOrCreate()
    # Encode each route as one row using spark
    encoded_spark_df, product_list = encode_data(spark, pd_df, False)
    # Convert back to pandas for processing
    encoded_pd_df = encoded_spark_df.toPandas()

    def flatten_dict(row):
        flat_dict = {}
        for col in row.index:
            if isinstance(row[col], dict):
                for key, value in row[col].items():
                    flat_dict[f"{col}_{key}"] = float(value)
        return pd.Series(flat_dict)
    # Flatten the dataframe that contains dicts
    df_flattened = encoded_pd_df.apply(flatten_dict, axis=1)

    # Fill missing values with 0's
    df_flattened = df_flattened.fillna(0)

    # Perform dimensionality reduction by first scaling
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df_flattened)

    # Perform PCA ->
    # PCA (Principal Component Analysis) is a technique used for dimensionality
    # reduction that identifies the axes in the data space along which the data
    # varies the most, and uses these axes to reorient the data,
    # thereby preserving the maximum amount of variation in the data.
    pca = PCA(n_components=2)
    df_2d_pca = pca.fit_transform(df_scaled)

    # Convert back to DataFrame for easy handling
    df_2d_pca = pd.DataFrame(df_2d_pca, columns=["PC1", "PC2"])
    print(df_2d_pca.to_string())

    plt.figure(figsize=(16, 10))
    plt.scatter(df_2d_pca['PC1'], df_2d_pca['PC2'])
    plt.title('Scatter plot of PCA')
    plt.xlabel('PC1')
    plt.ylabel('PC2')
    plt.show()

    # Perform t-SNE ->
    # t-SNE (t-Distributed Stochastic Neighbor Embedding) is a
    # nonlinear dimensionality reduction technique that is
    # particularly good at preserving local structure in the data,
    # meaning that points which are close to each other in the
    # high-dimensional space remain close to each other in the
    # low-dimensional representation.
    tsne = TSNE(n_components=2)
    df_2d_tsne = tsne.fit_transform(df_scaled)

    # Convert back to DataFrame for easy handling
    df_2d_tsne = pd.DataFrame(df_2d_tsne, columns=["Dim1", "Dim2"])

    plt.figure(figsize=(16, 10))
    plt.scatter(df_2d_tsne['PC1'], df_2d_tsne['PC2'])
    plt.title('Scatter plot of t-SNE')
    plt.xlabel('PC1')
    plt.ylabel('PC2')
    plt.show()



