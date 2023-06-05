from typing import List
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import numpy as np
from statistics import mode

from pyspark.sql import SparkSession
import scipy

from pyspark import RDD
from pyspark import SparkContext
from pyspark.sql.functions import collect_list

#from clustering import run_clustering

from parse_data import parse_json_data, encode_data

from data_visualization import plot_routes, plot_results

from distance_function import route_distance


#Imports from the clustering file now added to the main_tests due to the extremely weird error.
import cmath as math
import numpy as np
from statistics import mode
from pyspark import RDD
from pyspark.sql import SparkSession



def run_all_tests():
    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [2, 3],
        'max_iterations': 2,
        'debug_flag': True,
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
        data=encoded_spark_rdd
        )
    print("The centroids are given by: ", centroids)

    # print("Start evaluating clusters")
    return

def plot_test():
    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [2, 3],
        'max_iterations': 2,
        'debug_flag': True,
    }

    # main function which runs all other tests imported from different files
    spark = SparkSession.builder.appName("Clustering").getOrCreate()
    print("Initialized Spark.")

    # Opletten dat bij het parsen de hoeveelheden van stad A-> stad B wel goed samengevoegd worden. Zie nu twee keer dezelfde from->to staan bij route 1 namelijk.
    pd_df, num_routes = parse_json_data()
    encoded_spark_df, product_list = encode_data(spark, pd_df, clustering_settings["debug_flag"])
    plot_routes(encoded_spark_df)





def run_clustering(spark_instance: SparkSession, clustering_settings: dict, data: RDD) -> list[tuple]:
    '''Define variables to store results.'''
    # E.g. for kmodes: [(predicted_centroids, (k, init_mode)), ...]
    results = []

    # Check which clustering algortihm to run
    if clustering_settings['clustering_algorithm'] == 'kmodes':
        for current_k in clustering_settings['k_values']:
            # TODO in the future add other parameters here.

            # Run clustering with current parameters
            predicted_centroids = kModes(
                spark_instance=spark_instance,
                data=data,
                k=current_k,
                clustering_settings=clustering_settings
            )

            # Store the settings, model, and metrics
            results.append((predicted_centroids, {'k': current_k}))
    else:
        print("Clustering algorithm setting not recognized in run_and_tune().")

    return results


def kModes(spark_instance: SparkSession, data: RDD, k: int, clustering_settings) -> list:
    """
    Perform k-modes clustering on the given data. Assumes only one-hot encoded data?

    Args:
        distance (function): The distance function to use for clustering.
        data (RDD): The RDD containing the data to cluster.
        k (int): The number of clusters to create.
        max iterations (int): The maximum number of iterations to perform.

    Returns:
        list: A list of the centroids of the clusters.
    """

    centroids = [x for x in data.takeSample(withReplacement=False, num=k)]

    # Iterate until convergence or until the maximum number of iterations is reached
    for i in range(clustering_settings["max_iterations"]):
        if clustering_settings["debug_flag"]:
            print("centroids = ", centroids)

        # Assign each point to the closest centroid
        clusters = data.map(
            lambda point: (min(centroids, key=lambda centroid: route_distance(point, centroid)), point)).groupByKey()

        print("clusters1 = ", clusters.collect())

        # Compute new centroids as the mode of the points in each cluster.
        newCentroids = clusters.mapValues(lambda arrays: tuple([mode(x) for x in zip(*arrays)])).collect()

        # print("newCentroids = ", newCentroids)

        # Update centroids
        for oldCentroid, newCentroid in newCentroids:
            index = centroids.index(oldCentroid)
            centroids[index] = newCentroid

    return [list(x) for x in centroids]