from typing import List
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import numpy as np
from statistics import mode

from pyspark.sql import SparkSession
import scipy

from pyspark import RDD
from pyspark import SparkContext

# Parameter search imports
from pyspark.ml import Estimator, Model
from pyspark.ml.param import Param, Params
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from pyspark.sql import SparkSession

def run_clustering(spark_instance: SparkSession, clustering_settings: dict, data: RDD) -> list[tuple]:
    '''Define variables to store results.'''
    # E.g. for kmodes: [(predicted_centroids, (k, init_mode)), ...]
    results = []

    # Check which clustering algortihm to run
    if clustering_settings['clustering_algorithm'] == 'kmodes':
        for k in clustering_settings['k_values']:
            for init_mode in clustering_settings['init_modes']:
                # Run clustering with current parameters
                predicted_centroids = kmodes(
                    spark_instance=spark_instance,
                    k=k,
                    init_mode=clustering_settings['init_mode'],
                    max_iter=clustering_settings['max_iter']
                )
    
                # Store the settings, model, and metrics
                results.append( (predicted_centroids, (k, init_mode)) )
    else:
        print("Clustering algorithm setting not recognized in run_and_tune().")
    
    return results





# Define a custom distance function
def jaccard_distance(a, b):
    a = np.array(a)
    b = np.array(b)
    intersection = np.sum(a & b)
    union = np.sum(a | b)
    return 1 - (intersection / union)


def kModes_v2(spark_instance: SparkSession,distance, data: RDD, k: int, maxIterations: int, list_size: int) -> list:
    """
    Perform k-modes clustering on the given data. Assumes only one-hot encoded data?

    Args:
        distance (function): The distance function to use for clustering.
        data (RDD): The RDD containing the data to cluster.
        k (int): The number of clusters to create.
        maxIterations (int): The maximum number of iterations to perform.
        list_size (int): The size of the lists in the data.

    Returns:
        list: A list of the centroids of the clusters.
    """
    # Initialize centroids randomly
    centroids = [tuple(x) for x in data.takeSample(withReplacement=False, num=k)]

    # Iterate until convergence or until the maximum number of iterations is reached
    for i in range(maxIterations):
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

def evaluate_clustering(data: RDD, predicted_centroids: list, clustering_settings: dict, perfect_centroids = None) -> dict:
    """
    Evaluate the clustering of the given data using the given centroids and clustering setting.

    Args:
        data (RDD): The RDD containing the data to cluster.
        centroids (list): The centroids of the clusters.
        clustering_setting (str): The type of clustering algorithm to use. Currently supports "kModes" and "kMeans".
    Returns:
        dict: A dictionary with evaluation metrics.
    Raises:
        NameError: If the clustering setting is not recognized.
        NotImplementedError: If the clustering setting is recognized but not implemented.
    """

    # Check which evaluation function to use
    if clustering_settings['clustering_algorithm'] == "kmodes":
        evaluation_metrics = evaluate_kModes(data, predicted_centroids, perfect_centroids=perfect_centroids)
    elif clustering_settings['clustering_algorithm'] == "kMeans":
        raise NotImplementedError
    else:
        raise NameError(f"Clustering setting not recognized.")

    # return data.map(lambda point: distance(point, min(centroids, key=lambda centroid: distance(point, centroid)))).sum()
    return evaluation_metrics
    

def evaluate_kModes(data: RDD, centroids: list, distance = scipy.spatial.distance, perfect_centroids = None) -> dict:
    """
    Evaluate the clustering of the given data using the given centroids and k-modes algorithm.
    """
    # First, for each point, find its closest centroid and map it as (centroid, distance). Distance is calculated using NOTE scipy jaccard distance.
    # Then, froup by centroid, compute the average distance to centroid for each group. 
    closest_centroids = data.map(lambda point: (min(centroids, key=lambda centroid: distance(point, centroid)), distance(point, min(centroids, key=lambda centroid: distance(point, centroid)))))
    average_within_cluster_distance = closest_centroids.groupByKey().mapValues(lambda distances: sum(distances) / len(distances)).collectAsMap() # TODO: CHECK IF THIS IS CORRECT

    # Calculate distances between the centroids. TODO: Check if this should not be the average over amount of clusters. Or, if this needs a more manual method using the dstiance function
    between_cluster_distance = scipy.spatial.distance.pdist(centroids, metric='jaccard') 

    # Calculate within cluster variance for each cluster (aka: sum of squared distance to centroid, averaged over amount of points per cluster.)
    within_cluster_variance = closest_centroids.map(lambda point: point[1]**2).mean()

    # If perfect_centroids are given, calculate average deviation from perfection
    if perfect_centroids:
        average_centroid_deviation = sum([distance(a, b) for a, b in zip(centroids, centroids)]) / len(centroids)
    else:
        average_centroid_deviation = None
    
    # # Calculate within cluster variance for each cluster, averaged over amount of points per cluster
    # within_cluster_variance = data.map(lambda point: scipy.spatial.distance.jaccard([point], centroids, metric='jaccard')).collect() # TODO: Check if this is correct

    return {
        'average_within_cluster_distance': average_within_cluster_distance,
        'between_cluster_distance': between_cluster_distance,
        'within_cluster_variance': within_cluster_variance,
        'average_centroid_deviation': average_centroid_deviation,
    }

def clustering_test1():
    # Testing code
    spark = SparkSession.builder.appName("Clustering").getOrCreate()

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
    # Cluster the data into two clusters using the k-modes algorithm with a custom distance function.

    centroids = kModes_v2(scipy.spatial.distance.jaccard, data, k=2, maxIterations=2, list_size = 5)

    print("Finished clustering. Start evaluation.")
    # Print the resulting centroids
    # for centroid in centroids:
    #     print(centroid)

    print("done")

    return centroids

    # # Print the evaluation metrics
    # print(evaluate_clustering(data, centroids, clustering_setting='kModes'))

if __name__ == '__main__':
    clustering_test1()
    
    