from typing import List
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import numpy as np
from statistics import mode

from pyspark.sql import SparkSession
import scipy

from distance_function import route_distance

from pyspark import RDD
from pyspark import SparkContext
from pyspark.sql.functions import collect_list

from clustering import run_clustering

from parse_data import parse_json_data, encode_data

from data_visualization import plot_routes, plot_results

def run_all_tests():
    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [2, 3],
        'max_iterations': 2,
        'distance_function': route_distance,
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

    # print("Running run_clustering().")
    # centroids = run_clustering(
    #     spark_instance=spark,
    #     clustering_settings=clustering_settings,
    #     data=encoded_spark_rdd
    #     )
    # print("The centroids are given by: ", centroids)

    # print("Start evaluating clusters")
    return

def plot_test():
    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [2, 3],
        'max_iterations': 2,
        'distance_function': route_distance,
        'debug_flag': True,
    }

    # main function which runs all other tests imported from different files
    spark = SparkSession.builder.appName("Clustering").getOrCreate()
    print("Initialized Spark.")

    # Opletten dat bij het parsen de hoeveelheden van stad A-> stad B wel goed samengevoegd worden. Zie nu twee keer dezelfde from->to staan bij route 1 namelijk.
    pd_df, num_routes = parse_json_data()
    encoded_spark_df, product_list = encode_data(spark, pd_df, clustering_settings["debug_flag"])
    plot_routes(encoded_spark_df)




if __name__ == "__main__":
    run_all_tests()