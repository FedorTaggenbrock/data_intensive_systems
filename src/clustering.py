from statistics import mode
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

import numpy as np
import math


def run_clustering(clustering_settings: dict, data: RDD) -> list[tuple]:
    '''Define variables to store results.'''
    # E.g. for kmodes: [(predicted_centroids, (k, init_mode)), ...]
    results = []

    # Check which clustering algortihm to run
    if clustering_settings['clustering_algorithm'] == 'kmodes':
        for current_k in clustering_settings['k_values']:
            # TODO in the future add other parameters here.

            # Run clustering with current parameters
            predicted_centroids = kModes(
                data=data,
                k=current_k,
                clustering_settings=clustering_settings
            )

            # Store the settings, model, and metrics
            results.append((predicted_centroids, {'k': current_k}))
    else:
        print("Clustering algorithm setting not recognized in run_and_tune().")

    return results




def kModes(data: RDD, k: int, clustering_settings):
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
    def dictionary_distance(dict1, dict2):
        # This function computes the euclidean distance for dict representations of (sparse) vectors.
        # The get method is used to return a default value of 0 for keys that are not present in one of the dictionaries
        return math.sqrt(np.sum(
            [(int(float(dict1.get(product, 0))) - int(float(dict2.get(product, 0)))) ** 2 for product in
             set(dict1) | set(dict2)]))

    def route_distance(route1, route2):
        columns = route1.__fields__[1:]
        intersection = 0
        union = 0
        for column in columns:
            trip1 = any(route1[column])
            trip2 = any(route2[column])
            if trip1 or trip2:
                union += 1
                if trip1 and trip2:
                    intersection += dictionary_distance(route1[column], route2[column])
        return intersection / union if union != 0 else 0.0

    if clustering_settings["debug_flag"]:
        two_routes = data.take(2)
        print("Distance between route 1 and 2 is given by: ")
        print(route_distance(two_routes[0], two_routes[1]))

    centroids = [x for x in data.takeSample(withReplacement=False, num=k)]

    #Iterate until convergence or until the maximum number of iterations is reached
    for i in range(clustering_settings["max_iterations"]):
        # Assign each point to the closest centroid
        clusters = data.map(lambda point: (min(centroids, key=lambda centroid: route_distance(point, centroid)), point)).groupByKey()

        #Compute new centroids as the mode of the points in each cluster.
        newCentroids = clusters.mapValues(lambda arrays: tuple([mode(x) for x in zip(*arrays)]) ).collect()

        if clustering_settings["debug_flag"]:
            print("centroids = ", centroids)
            print("clusters1 = ", clusters.collect())
            print("newCentroids = ", newCentroids)

        # Update centroids
        for oldCentroid, newCentroid in newCentroids:
            index = centroids.index(oldCentroid)
            centroids[index] = newCentroid

    return [list(x) for x in centroids]



