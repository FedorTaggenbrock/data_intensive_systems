from typing import List
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import numpy as np
import random
from statistics import mode

from pyspark.sql import SparkSession
import scipy

from pyspark import RDD
from pyspark import SparkContext
from pyspark.sql import DataFrame

# Parameter search imports
from pyspark.ml import Estimator, Model
from pyspark.ml.param import Param, Params
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from pyspark.sql import SparkSession

def run_clustering(spark_instance: SparkSession, clustering_settings: dict, data: DataFrame) -> list[tuple]:
    '''Define variables to store results.'''
    # E.g. for kmodes: [(predicted_centroids, (k, init_mode)), ...]
    results = []

    # Check which clustering algortihm to run
    if clustering_settings['clustering_algorithm'] == 'kmodes':
        for current_k in clustering_settings['k_values']: 
            # TODO in the future add other parameters here.

            # Run clustering with current parameters
            predicted_centroids = kModes_v2(
                spark_instance=spark_instance,
                data=data,
                distance=clustering_settings['distance_function'],
                k=current_k,
                clustering_settings=clustering_settings
            )

            # Store the settings, model, and metrics
            results.append( (predicted_centroids, {'k':current_k}) )
    else:
        print("Clustering algorithm setting not recognized in run_and_tune().")
    
    return results


def kModes_v2(spark_instance: SparkSession, distance, data: RDD, k: int, clustering_settings) -> list:
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
        clusters = data.map(lambda point: (min(centroids, key=lambda centroid: distance(point, centroid)), point)).groupByKey()

        #print("clusters1 = ", clusters.collect())

        #Compute new centroids as the mode of the points in each cluster
        newCentroids = clusters.mapValues(lambda arrays: tuple([mode(x) for x in zip(*arrays)]) ).collect()

        #print("newCentroids = ", newCentroids)

        # Update centroids
        for oldCentroid, newCentroid in newCentroids:
            index = centroids.index(oldCentroid)
            centroids[index] = newCentroid

    return [list(x) for x in centroids]



def clustering_test1():
    # Testing code
    spark = SparkSession.builder.appName("Clustering").getOrCreate()
    # spark = SparkSession.builder.master("local").appName("Clustering").getOrCreate()

    data = spark.sparkContext.parallelize([
            [1,1,0,1,0],
            [1,1,1,1,0],
            [0,0,1,0,1],
            [1,0,0,0,1],
            [1,0,0,1,0],
            [1,1,1,1,0],
            [0,1,1,0,1],
            [1,0,0,1,0],
        ])

    print("Initialized Spark. Start clustering.")

    centroids = kModes_v2(
        spark_instance=spark,
        distance = scipy.spatial.distance.jaccard,
        data=data,
        k=2,
        max_iterations=2,
        )
    
    print("Finished clustering.")
    return centroids


if __name__ == '__main__':
    # clustering_test1()
    clustering_test2()	


    
    