from pyspark.sql import SparkSession
from parse_data import parse_json_data, encode_data
from data_visualization import plot_routes, convert_pd_df_to_one_row
from parse_data import parse_json_data, encode_data, get_data
from data_visualization import plot_routes
from clustering import run_clustering
from os import getcwd
import pandas as pd
from evaluate_clustering import evaluate_clustering, get_best_setting
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from distance_functions import test_distance_function
import matplotlib.pyplot as plt

def run_all_tests():
    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [10],
        'max_iterations': 5,
        'debug_flag': True
    }

    spark = SparkSession.builder.appName("Clustering").getOrCreate()

    actual_routes_rdd, num_routes = get_data(spark, 'data_intensive_systems/data/1000_actual_routes.json', clustering_settings)
    clustering_settings["num_actual_routes"] = num_routes

    print("Running run_clustering().")
    results = run_clustering(
        data=actual_routes_rdd,
        clustering_settings=clustering_settings
        )

    print("Start evaluating clusters")
    metrics = evaluate_clustering(actual_routes_rdd, results, clustering_settings)
    best_settings = get_best_setting(metrics)
    print("best settings are given by: \n", best_settings)
    return


def plot_test():
    # Load data and create data frame
    pd_df, num_routes = parse_json_data('data_intensive_systems/data/10000_actual_routes.json')
    encoded_pd_df = convert_pd_df_to_one_row(pd_df)

    pd_df_st, num_routes = parse_json_data('data_intensive_systems/data/10_0.25_standard_route.json')
    encoded_pd_df_st = convert_pd_df_to_one_row(pd_df_st)

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
    print(df_scaled.shape)
    # Flatten and scale the standard route
    df_st_flattened = encoded_pd_df_st.apply(flatten_dict, axis=1)
    df_st_flattened = df_st_flattened.fillna(0)
    df_st_scaled = scaler.fit_transform(df_st_flattened)

    # Perform PCA ->
    # PCA (Principal Component Analysis) is a technique used for dimensionality
    # reduction that identifies the axes in the data space along which the data
    # varies the most, and uses these axes to reorient the data,
    # thereby preserving the maximum amount of variation in the data.
    pca = PCA(n_components=2)
    # df_2d_pca = pca.fit_transform(df_scaled)

    # Convert back to DataFrame for easy handling
    # df_2d_pca = pd.DataFrame(df_2d_pca, columns=["PC1", "PC2"])
    # print(df_2d_pca.to_string())
    # Also for the standard routes
    df_st_2d_pca = pca.fit_transform(df_st_scaled)
    df_st_2d_pca = pd.DataFrame(df_st_2d_pca, columns=["PC1", "PC2"])


    plt.figure(figsize=(16, 10))
    # plt.scatter(df_2d_pca['PC1'], df_2d_pca['PC2'], color='blue')
    plt.scatter(df_st_2d_pca['PC1'], df_st_2d_pca['PC2'], color='red')
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
    tsne = TSNE(n_components=2, perplexity=3)
    # df_2d_tsne = tsne.fit_transform(df_scaled)

    # Convert back to DataFrame for easy handling
    # df_2d_tsne = pd.DataFrame(df_2d_tsne, columns=["Dim1", "Dim2"])

    # Also for the standard routes
    df_st_2d_tsne = tsne.fit_transform(df_st_scaled)
    df_st_2d_tsne = pd.DataFrame(df_st_2d_tsne, columns=["Dim1", "Dim2"])

    plt.figure(figsize=(16, 10))
    # plt.scatter(df_2d_tsne['Dim1'], df_2d_tsne['Dim1'])
    plt.scatter(df_st_2d_tsne['Dim1'], df_st_2d_tsne['Dim1'])
    plt.title('Scatter plot of t-SNE')
    plt.xlabel('PC1')
    plt.ylabel('PC2')
    plt.show()



