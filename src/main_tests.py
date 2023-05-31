from typing import List
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import numpy as np
from statistics import mode

from pyspark.sql import SparkSession
import scipy

from pyspark import RDD
from pyspark import SparkContext

from clustering import run_clustering

from parse_data import parse_json_data, encode_data

def run_all_tests():
    #main function which runs all other tests imported from different files
    spark = SparkSession.builder.appName("Clustering").getOrCreate()
    print("Initialized Spark.")

    pd_df = parse_json_data()
    spark_df = spark.createDataFrame(pd_df)
    print("Spark data frame : ")
    spark_df.show()

    encoded_pd_df = encode_data(pd_df, encode_style='all_')

    encoded_spark_df = spark.createDataFrame(encoded_pd_df)
    print("Encoded spark data frame : ")
    encoded_spark_df.show()


    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [2, 3],
        'max_iterations': 2,
        'distance_function': scipy.spatial.distance.jaccard,
        'debug_flag': False,
    }

    print("Running run_clustering().")
    centroids = run_clustering(
        spark_instance=spark,
        clustering_settings=clustering_settings,
        data=data,
        )
    print("The centroids are given by: ", centroids)

    print("Start evaluating clusters")


    return

if __name__ == "__main__":
    run_all_tests()