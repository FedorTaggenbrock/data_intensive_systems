# General modules
import numpy as np
import scipy.spatial.distance
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score

# Spark etc
from pyspark import RDD
from pyspark.sql import SparkSession

# Typing
from typing import Any, Callable, Union



def evaluate_clustering(data: RDD, clustering_result: list[tuple[ list[float], dict[str, Any]]], clustering_settings: dict, perfect_centroids = None) -> list[dict]:
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
        evaluation_metrics = evaluate_kModes2(data=data, 
                                             clustering_settings=clustering_settings,
                                             clustering_result=clustering_result,
                                             perfect_centroids=perfect_centroids)
    elif clustering_settings['clustering_algorithm'] == "kMeans":
        raise NotImplementedError
    else:
        raise NameError(f"Clustering setting not recognized.")

    # return data.map(lambda point: distance(point, min(centroids, key=lambda centroid: distance(point, centroid)))).sum()
    return evaluation_metrics
    


def evaluate_kModes2(data: RDD, clustering_settings: dict, clustering_result: list[tuple[ list[float], dict[str, Any]]], 
                    perfect_centroids: Union[None, tuple]):
    f"""
    Evaluate the clustering of the given data using the given centroids and provided distance function.
    Evaluation is done for each clustering setting, with the entire data. The following metrics are calculated::
    - average_within_cluster_distance
    - average_within_cluster_variance
    - average_centroid_deviation
    - Silhouette Score: The silhouette score is a measure of how similar an object is to its own cluster (cohesion) compared to other clusters (separation). The silhouette score ranges from -1 to 1, where a high value indicates that the object is well matched to its own cluster and poorly matched to neighboring clusters.
    - Davies-Bouldin Index: This index is a metric of cluster separation. It calculates the average similarity measure of each cluster with its most similar cluster, where similarity is the ratio of within-cluster distances to between-cluster distances. Lower values indicate better clustering.
    - Calinski-Harabasz Index: Also known as the Variance Ratio Criterion, this index is a ratio of the between-cluster dispersion mean and the within-cluster dispersion. Higher values indicate better clustering.

    Args:
        clustering_results: For each clustering setting, contains predicted centroids,
        e.g. for kmodes: [([1.0, 2.0, 2.5], {'k':2}), ...], aka List[Tuple[ List[float], Dict[str, Any]]]
    """
    # Store the results etc
    results = []
    distance_function = clustering_settings['distance_function']
    
    # Convert the spark rdd data to numpy compatible format
    data_np = np.array(data.collect())  # collect data from RDD to numpy array

    # For each clustering setting, calculate the metrics
    for centroids, setting in clustering_result:
        # Assign each datapoint to its closest centroid
        distances_to_centroids = np.array([distance_function(data_np, centroid) for centroid in centroids])
        labels = np.argmin(distances_to_centroids, axis=0)

        # Calculate average distances
        average_within_cluster_distance = np.mean([distance_function(data_np[labels == i], centroids[i]) for i in range(len(centroids))])
        average_within_cluster_variance = np.var([distance_function(data_np[labels == i], centroids[i]) for i in range(len(centroids))])
        average_centroid_deviation = np.mean(np.std(centroids, axis=0))

        # Calculate Silhouette Score, Davies-Bouldin Index and Calinski-Harabasz Index
        silhouette = silhouette_score(data_np, labels)
        db_index = davies_bouldin_score(data_np, labels)
        ch_index = calinski_harabasz_score(data_np, labels)

        # Store the metrics in a dictionary
        metrics = {
            'settings': setting,
            'average_within_cluster_distance': average_within_cluster_distance,
            'average_within_cluster_variance': average_within_cluster_variance,
            'average_centroid_deviation': average_centroid_deviation,
            'silhouette_score': silhouette,
            'davies_bouldin_index': db_index,
            'calinski_harabasz_index': ch_index
        }

        results.append(metrics)

    return results


def evaluate_clustering_test2(clustering_result):

    spark = SparkSession.builder.\
            config('spark.app.name', 'evaluate_clustering_test1').\
                getOrCreate()

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
    
    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [2, 3],
        'max_iterations': 2,
        'distance_function': scipy.spatial.distance.jaccard,
        'debug_flag': False,
    }

    metrics = evaluate_clustering(
        data=data,
        clustering_result=clustering_result,
        clustering_settings=clustering_settings,
        perfect_centroids=None
    )

    spark.stop()

    return metrics

if __name__ == "__main__":
    dummy_result = [([[1, 0, 0, 1, 0], [1, 1, 1, 1, 0]], {'k': 2}), ([[0, 0, 1, 0, 1], [1, 1, 1, 1, 0], [1, 0, 0, 1, 0]], {'k': 3})]
    res = print(evaluate_clustering_test2(dummy_result)	)

 
 def evaluate_kModes(data: RDD, clustering_settings: dict, clustering_result: list[tuple[ list[float], dict[str, Any]]], 
                    perfect_centroids: Union[None, tuple]) -> dict:
    f"""
    Evaluate the clustering of the given data using the given centroids and k-modes algorithm.

    Args:
        clustering_results: For each clustering setting, contains predicted centroids,
        e.g. for kmodes: [([1.0, 2.0, 2.5], {'k':2}), ...], aka List[Tuple[ List[float], Dict[str, Any]]]

    """

    results = []
    distance_function = clustering_settings['distance_function']

    for result in clustering_result:
        # Extract the centroids for this setting
        centroids = result[0]
        num_clusters = len(centroids)

        # For each point, find its closest centroid and map it as a tuple where (centroid, point).
        # Example structure:      (centroid       , point          )
        #                      [  ([1, 1, 1, 1, 0], [1, 1, 0, 1, 0]),
        #                         ([1, 0, 0, 1, 0], [1, 0, 0, 0, 1]), ... ]
        closest_centroids = data.map(lambda point: (min(centroids, key=lambda centroid: distance_function(point, centroid)), point))
        

        # Group points by centroid and compute the total distance and count.
        # Example structure: {centroid1: {'total_distance': 30,
        #                                 'count': 10, 
        #                                 'distances': [1, 2, 3, 2, 3, 2, 1, 2, 3, 1]},
        #                     centroid2: {...}, ...}
        # TODO THIS DOES NOT WORK YET
        cluster_metrics = closest_centroids.groupByKey().mapValues(lambda key, values: {
            # 'total_distance': sum([v[1] for v in values]), # Maybe use np.sum() instead? Or perhaps datastructure is just different. TODO
            'count': len(values), # should be converted to list, to make sure len() is there
            'distances': [v[1] for v in values]   # we also need the list of distances to calculate the variance
        }).collectAsMap()

        # Compute the average distance within each cluster, for each centroid
        for key in cluster_metrics:
            cluster_metrics[key]['average_distance'] = cluster_metrics[key]['total_distance'] /   cluster_metrics[key]['count'] # type: ignore
        # Compute the overall average distance across all clusters (aka sum the within-cluster-distances and divide by the number of clusters)
        average_within_cluster_distance = np.sum([v['average_distance'] for v in cluster_metrics.values()]) / num_clusters

        # Compute the variance within each cluster
        for key in cluster_metrics:
            cluster_metrics[key]['variance'] = np.var(cluster_metrics[key]['distances'])
        # Compute the overall average variance across all clusters
        average_within_cluster_variance = np.sum([v['variance'] for v in cluster_metrics.values()]) / num_clusters

        # Calculate distances between the centroids
        '''Possible options are:
        - Silhouette Score: The silhouette score is a measure of how similar an object is to its own cluster (cohesion) compared to other clusters (separation). The silhouette score ranges from -1 to 1, where a high value indicates that the object is well matched to its own cluster and poorly matched to neighboring clusters.
        - Davies-Bouldin Index: This index is a metric of cluster separation. It calculates the average similarity measure of each cluster with its most similar cluster, where similarity is the ratio of within-cluster distances to between-cluster distances. Lower values indicate better clustering.
        - Calinski-Harabasz Index: Also known as the Variance Ratio Criterion, this index is a ratio of the between-cluster dispersion mean and the within-cluster dispersion. Higher values indicate better clustering.
        '''

        # If perfect_centroids are given, calculate average deviation from perfection
        if perfect_centroids:
            average_centroid_deviation = sum([distance_function(a, b) for a, b in zip(centroids, perfect_centroids)]) / len(centroids)
        else:
            average_centroid_deviation = None

        results.append({
            'settings': result[1],
            'average_within_cluster_distance': average_within_cluster_distance,
            'average_within_cluster_variance': average_within_cluster_variance,
            # Add between-cluster metrics here
            'average_centroid_deviation': average_centroid_deviation,
        })

    return results