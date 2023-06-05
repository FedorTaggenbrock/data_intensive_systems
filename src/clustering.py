from statistics import mode
from pyspark import RDD
from pyspark.sql import SparkSession
from distance_function import route_distance
from os import getenv

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
            results.append( (predicted_centroids, {'k':current_k}) )
    else:
        print("Clustering algorithm setting not recognized in run_and_tune().")
    
    return results


def kModes(spark_instance: SparkSession, data: RDD, k: int, clustering_settings):
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
    # two_routes = data.take(2)
    # print("The distance between route 0 and route 1!! is given by:")
    # print(route_distance(two_routes[0], two_routes[1]))
    #
    # return []

    print(getenv())

    # Iterate until convergence or until the maximum number of iterations is reached
    for i in range(clustering_settings["max_iterations"]):
        if clustering_settings["debug_flag"]:
            print("centroids = ", centroids)
        # Assign each point to the closest centroid
        clusters = data.map(lambda point: (min(centroids, key=lambda centroid: route_distance(point, centroid)), point)).groupByKey()
        print("clusters1 = ", clusters.collect())

    return []
    #
    #     #Compute new centroids as the mode of the points in each cluster.
    #     newCentroids = clusters.mapValues(lambda arrays: tuple([mode(x) for x in zip(*arrays)]) ).collect()
    #
    #     #print("newCentroids = ", newCentroids)
    #
    #     # Update centroids
    #     for oldCentroid, newCentroid in newCentroids:
    #         index = centroids.index(oldCentroid)
    #         centroids[index] = newCentroid
    #
    # return [list(x) for x in centroids]





    
    