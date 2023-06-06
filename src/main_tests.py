from pyspark.sql import SparkSession
from parse_data import parse_json_data, encode_data
from data_visualization import plot_routes
from distance_function import route_distance
from clustering import run_clustering
from os import getcwd
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

print(getcwd())
def run_all_tests():
    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [2, 3],
        'max_iterations': 2,
        'debug_flag': True,
        'distance_func' : route_distance
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
        two_routes = encoded_spark_rdd.take(2)
        print("The distance between route 0 and route 1 is given by:")
        print(route_distance(two_routes[0], two_routes[1]))

    print("Running run_clustering().")
    centroids = run_clustering(
        spark_instance=spark,
        clustering_settings=clustering_settings,
        data=encoded_spark_rdd,
        )
    print("The centroids are given by: ", centroids)

    # print("Start evaluating clusters")
    return

def plot_test():
    # Opletten dat bij het parsen de hoeveelheden van stad A-> stad B wel goed samengevoegd worden. Zie nu twee keer dezelfde from->to staan bij route 1 namelijk.
    pd_df, num_routes = parse_json_data()
    spark = SparkSession.builder.appName("Clustering").getOrCreate()
    print("Initialized Spark.")
    encoded_spark_df, product_list = encode_data(spark, pd_df, False)
    encoded_pd_df = encoded_spark_df.toPandas()
    # print(encoded_pd_df.to_string())

    def flatten_dict(row):
        flat_dict = {}
        for col in row.index:
            if isinstance(row[col], dict):
                for key, value in row[col].items():
                    flat_dict[f"{col}_{key}"] = float(value)
        return pd.Series(flat_dict)

    df_flattened = encoded_pd_df.apply(flatten_dict, axis=1)

    # Step 2: Fill missing values with 0
    df_flattened = df_flattened.fillna(0)

    # Step 3: Perform dimensionality reduction

    # Scale the data
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df_flattened)

    # Perform PCA
    pca = PCA(n_components=2)
    df_2d = pca.fit_transform(df_scaled)

    # Convert back to DataFrame for easy handling
    df_2d = pd.DataFrame(df_2d, columns=["PC1", "PC2"])
    print(df_2d.to_string())

    plt.figure(figsize=(16, 10))
    plt.scatter(df_2d['PC1'], df_2d['PC2'])
    plt.title('Scatter plot of PC1 vs PC2')
    plt.xlabel('PC1')
    plt.ylabel('PC2')
    plt.show()



