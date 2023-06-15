# General modules
import numpy as np
import scipy.spatial.distance
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
import pickle

# Spark etc
from pyspark import RDD
from pyspark.sql import SparkSession

# Typing
from typing import Any, Callable, Union

# Own module imports (testing)
from parse_data_3 import get_data_3

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
        evaluation_metrics = evaluate_kModes(data=data,
                                             clustering_settings=clustering_settings,
                                             clustering_result=clustering_result,
                                             perfect_centroids=perfect_centroids)
    elif clustering_settings['clustering_algorithm'] == "kMeans":
        raise NotImplementedError
    else:
        raise NameError(f"Clustering setting not recognized.")

    if clustering_settings["debug_flag"]:
        print("The evaluation metrics are given by:", evaluation_metrics)

    # return data.map(lambda point: distance(point, min(centroids, key=lambda centroid: distance(point, centroid)))).sum()
    return evaluation_metrics
    


def evaluate_kModes(data: RDD, clustering_settings: dict, clustering_result: list[tuple[ list[float], dict[str, Any]]],
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
        distances_to_centroids = np.array([[distance_function(datapoint, centroid) for centroid in centroids] for datapoint in data_np])
        labels = np.argmin(distances_to_centroids, axis=1)

        # Calculate average distances
        average_within_cluster_distance = np.mean([np.mean(distances_to_centroids[labels == i, i]) for i in range(len(centroids))])
        average_within_cluster_variance = np.var([np.var(distances_to_centroids[labels == i, i]) for i in range(len(centroids))])


        # Vectorized attempt
        """
        average_within_cluster_distance = np.mean([np.mean(distance_function(data_np[labels == i], centroids[i])) for i in range(len(centroids))])
        average_within_cluster_variance = np.var([np.var(distance_function(data_np[labels == i], centroids[i])) for i in range(len(centroids))])
        """
        

        # Calculate Silhouette Score, Davies-Bouldin Index and Calinski-Harabasz Index
        silhouette = silhouette_score(data_np, labels, metric=distance_function)
        # TODO these assume euclidean distance
        db_index = davies_bouldin_score(data_np, labels)
        ch_index = calinski_harabasz_score(data_np, labels)

        if perfect_centroids is not None:
            # Calculate deviation of centroids from perfect centroids. First, calculate for each centroid which the distance to each perfect centroid.
            distances_to_perfect_centroids = np.array([  [distance_function(centroid, perfect_centroid) for perfect_centroid in perfect_centroids]  for centroid in centroids])
            
            # And then, determine which is closest.
            closest_perfect_centroids_index = np.argmin(distances_to_perfect_centroids, axis=1)
            closest_perfect_centroids = np.array([perfect_centroids[i] for i in closest_perfect_centroids_index])

            # Then, calculate the average distance between each centroid and its closest perfect centroid
            centroid_to_closest_perfect_centroid_distance = np.array([distance_function(centroid, closest_perfect_centroid) for centroid, closest_perfect_centroid in zip(centroids, closest_perfect_centroids)]) 
            average_centroid_to_closest_perfect_centroid_distance = np.mean(centroid_to_closest_perfect_centroid_distance)

            # Store the result
            average_centroid_deviation = average_centroid_to_closest_perfect_centroid_distance
        else:
            average_centroid_deviation = None

        # Store the metrics in a dictionary
        metrics = {
            'settings': setting,
            'average_within_cluster_distance': average_within_cluster_distance,
            'average_within_cluster_variance': average_within_cluster_variance,
            'average_centroid_deviation': average_centroid_deviation,
            'silhouette_score': silhouette,
            # 'davies_bouldin_index': db_index,
            # 'calinski_harabasz_index': ch_index
        }

        results.append(metrics)

    return 



def evaluate_clustering_test3():
    spark = SparkSession.builder.appName("Clustering").getOrCreate()

    def load_results(path='data/serialized_results_for_debugging/results.pkl'):
        with open(path, 'rb') as f:
            return pickle.load(f)
    
    pickled_clustering_result = load_results()

    actual_routes_rdd, num_routes = get_data_3(spark, 'data_intensive_systems/data/1000_0.25_actual_routes.json')

    metrics = evaluate_clustering(
        data=data,
        clustering_result=pickled_clustering_result,
        clustering_settings=clustering_settings,
        perfect_centroids=perfect_centroids,
    )



    return

def evaluate_clustering_test2():

    spark = SparkSession.builder.appName("Clustering").getOrCreate()

    data = spark.sparkContext.parallelize([
            [1,1,1,1,1],
            [0,0,0,0,0],
            [1,1,1,1,0],
            [0,0,1,1,0],
            [1,0,1,1,0],
            [0,0,0,1,0],
            [1,1,1,0,0],
            [1,1,0,0,0],
        ])

    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [2, 3],
        'max_iterations': 2,
        'distance_function': scipy.spatial.distance.jaccard,
        'debug_flag': False,
    }

    perfect_centroids = [[1, 1, 1, 1, 0], [0, 0, 1, 1, 0], [1, 0, 0, 1, 0]]
    dummy_result = [
        ([[1, 0, 0, 1, 0], [1, 1, 1, 1, 0]], {'k': 2}),
        ([[0, 0, 1, 0, 1], [1, 1, 1, 1, 0], [1, 0, 0, 1, 0]], {'k': 3}),
        ([[0, 0, 1, 0, 1], [1, 1, 1, 1, 0], [1, 0, 0, 1, 0], [1,1,0,1,0]], {'k': 4})
    ]

    metrics = evaluate_clustering(
        data=data,
        clustering_result=dummy_result,
        clustering_settings=clustering_settings,
        perfect_centroids=perfect_centroids,
    )

    spark.stop()

    return metrics

def get_best_setting(metrics: list[dict]) -> dict:
    """
    Given a list of metrics for different clustering settings, return the best setting.
    We have several metrics, and want to find the best setting for each metric. We do this using the elbow method.
    The elbow occurs when the first derivative has its maximum (we do assume the series to be monotically decreasing, this an inherent assumption of the elbow method ),
    in other words when the second derivative is zero. So we approximate the second derivative. 
    """

    # Sort the list by the setting k. Note, they should be sorted by default already.
    sorted_metrics = sorted(metrics, key=lambda x: x['settings']['k'])
    # Store which metrics are better when they are higher, as the elbow is calculated differently for increasing series.
    higher_better_metrics = ['silhouette_score']
    # Final result, to be returned.
    best_settings_dict = {}

    for metric_name in sorted_metrics[0].keys():
        if metric_name == 'settings': # Skip the settings; those are not a type of metric
            continue
        elif sorted_metrics[0][metric_name] is None: # This should only be the case for average_centroid_deviation (which is calculated by comparing to perfect centroids).
            continue
        else:
            # Get just the scores for this metric
            scores = [metric[metric_name] for metric in sorted_metrics]

            if len(scores)<=2:
                # We can't calculate the second derivative then, so just take the first setting.
                elbow_point_index = 0
            else:

                # Approximate first derivative
                diff_1 = np.diff(scores)

                # Approximate second derivative. For metrics that should be minimized, like within cluster distance, we look for the elbow where 
                # the decrease starts to slow down, so we compute the second derivative and find, where it is maximum.
                # For metrics that should be maximized, like silhouette score, we look for the elbow where the increase starts to slow down,
                # so we compute the second derivative and find where it is minimum.
                if metric_name in higher_better_metrics:
                    diff_2 = np.diff(diff_1)
                else:
                    diff_2 = -np.diff(diff_1)
            
                elbow_point_index = np.argmax(diff_2) + 2 # Need to add 2, because diff starts with: list[1]-list[0], so the first element is "lost."
                
            # Store the best setting for this metric
            best_setting = sorted_metrics[elbow_point_index]['settings']
            best_settings_dict[metric_name] = best_setting

            """OLD CODE
            # Approximate first derivative (difference between each consecutive score)
            first_derivative = [scores[i+1] - scores[i] for i in range(len(scores) - 1)]

            # Approximate second derivative (difference between each consecutive first derivative)
            second_derivative = [first_derivative[i+1] - first_derivative[i] for i in range(len(first_derivative) - 1)]

            # Find the index of the first local minimum or maximum in the second derivative,
            # depending on whether the metric should be minimized or maximized.
            if metric_name in higher_better_metrics:
                # We're looking for a maximum in the first derivative, which is the rate of INCREASE in this case.
                # This is where the second derivative changes sign, aka a local MAXIMUM in the second derivative.
                # This indicates the elbow.
                elbow_point_index = next(i for i in range(len(second_derivative) - 1) if second_derivative[i] > 0 and second_derivative[i+1] < 0)
            else:
                # Here, it's almost the same, but we have the rate of DECREASE as the first derivative.
                # So, we're looking for a local MINIMUM in the second derivative.
                elbow_point_index = second_derivative.index(min(second_derivative)) + 1
            """
    return best_settings_dict

    # Lower is better



    lowest_within_cluster_distance_sort = sorted(metrics, key=lambda x: x['average_within_cluster_distance'])

    best_setting_within_cluster_distance = lowest_within_cluster_distance_sort[0]['settings']
    # Lower is better
    lowest_within_cluster_variance_sort = sorted(metrics, key=lambda x: x['average_within_cluster_variance'])
    best_setting_within_cluster_variance = lowest_within_cluster_variance_sort[0]['settings']
    # Lower is better
    lowest_centroid_deviation_sort = sorted(metrics, key=lambda x: x['average_centroid_deviation'])
    best_setting_within_cluster_variance = lowest_centroid_deviation_sort[0]['settings']
    # Higher is better
    best_setting_silhoutte_score = sorted(metrics, key=lambda x: x['silhouette_score'], reverse=True)[0]['settings']

    # Now, we want to calculate the best setting according to the elbow method. We don't want to do this visually.
    # We want to find the point where the average within cluster distance starts to decrease less rapidly.
    # We do this by approximating the second derivative of the average within cluster distance.
    
    # First, get a list all the average within cluster distances in their actual order. 
    average_within_cluster_distances = [metric['average_within_cluster_distance'] for metric in metrics]


if __name__ == "__main__":

    res = evaluate_clustering_test2()
    print('\n', res, '\n')

    dummy_res = [
        {'settings':{'k': 2},
         'average_within_cluster_distance': 0.5166666666666666,
         'average_within_cluster_variance': 0.0002551118827160493,
         'average_centroid_deviation': 0.25,
         'silhouette_score': 0.30505357142857137},
        {'settings':{'k': 3},
         'average_within_cluster_distance':0.5944444444444444,
         'average_within_cluster_variance':0.00020747599451303157,
         'average_centroid_deviation':0.38888888888888884,
         'silhouette_score':0.25358420593368236},
        {'settings': {'k': 4},
         'average_within_cluster_distance': 0.5183333333333333,
         'average_within_cluster_variance': 0.00012096749999999996,
         'average_centroid_deviation': 0.22916666666666666,
         'silhouette_score': 0.0993585317061738}         
         ]
    
    best_setting = get_best_setting(dummy_res)
    print(best_setting)

    print('done')

 
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
