from typing import Callable, Union
import numpy as np
import scipy.spatial.distance
from pyspark import RDD


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
    

def evaluate_kModes(data: RDD, clustering_results: list, distance: Callable, perfect_centroids = Union[None, tuple]) -> dict:
    """
    Evaluate the clustering of the given data using the given centroids and k-modes algorithm.

    Args:
        clustering_results: For each clustering setting, contains predicted centroids, e.g. for kmodes: [(predicted_centroids, (k, init_mode)), ...].
    """

    results = []

    for result in clustering_results:
        # Extract the centroids for this setting
        centroids: tuple = result[0]

        # For each point, find its closest centroid and map it as (centroid, (distance, 1))
        closest_centroids = data.map(lambda point: (min(centroids, key=lambda centroid: distance(point, centroid)), (distance(point, min(centroids, key=lambda centroid: distance(point, centroid))), 1)))

        # Compute the average and variance of the distances to each centroid
        sum_distances_and_counts = closest_centroids.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

        average_within_cluster_distance = sum_distances_and_counts.mapValues(lambda x: x[0] / x[1]).collectAsMap()
        within_cluster_variance = sum_distances_and_counts.mapValues(lambda x: (x[0] / x[1]) ** 2).collectAsMap()

        # Calculate distances between the centroids
        between_cluster_distance = np.mean(distance.pdist(centroids, metric='jaccard'))

        # If perfect_centroids are given, calculate average deviation from perfection
        if perfect_centroids:
            average_centroid_deviation = sum([distance(a, b) for a, b in zip(centroids, perfect_centroids)]) / len(centroids)
        else:
            average_centroid_deviation = None

        results.append({
            'settings': result[1],
            'average_within_cluster_distance': average_within_cluster_distance,
            'between_cluster_distance': between_cluster_distance,
            'within_cluster_variance': within_cluster_variance,
            'average_centroid_deviation': average_centroid_deviation,
        })

    return results