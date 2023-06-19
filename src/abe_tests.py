# General modules
import numpy as np
import scipy.spatial.distance
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
import pickle
import sys
import os

# Spark etc
from pyspark import RDD
from pyspark.sql import SparkSession

# Typing
from typing import Any, Callable, Union

# Own stuff
from pyspark.sql import SparkSession
import pandas as pd


# from data_visualization import plot_routes, convert_pd_df_to_one_row
from clustering import run_clustering, route_distance
from parse_data import get_nested_data, get_vector_dataframe, get_data
from evaluate_clustering import evaluate_clustering, get_best_setting


def evaluate_clustering_test3():
    spark = SparkSession.builder.appName("Clustering").getOrCreate()
    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [3],
        'distance_function': route_distance,
        'max_iterations': 4,
        'debug_flag': True,
    }

    _ON_COLAB = 'google.colab' in sys.modules
    general_pickled_path = r'data\serialized_results_for_debugging\results__algo=kmodes_kvalues=[3]_max_iter=4.pkl'
    general_perfect_centroids_path = r'data\data_12_06\10_standard_route.json'
    if _ON_COLAB:
        pickled_path = '/content/data_intensive_systems/' + general_pickled_path.replace("\\", "/")
        perfect_centroids = '/content/data_intensive_systems/data/10_standard_routes.json'
    else:
        pickled_path = os.getcwd() + general_pickled_path
        perfect_centroids= os.getcwd() + '\\data\\10_standard_routes.json'
    
    pickled_clustering_result = load_results(pickled_path)

    results = pickled_clustering_result[0]
    metrics = pickled_clustering_result[1]

    if not metrics: # then kmodes
        actual_routes_rdd, indices2from_to_prods = get_data(spark, 'data_intensive_systems/data/1000_0.25_actual_routes.json', clustering_settings)
        # perfect centroids

        results, metrics = evaluate_clustering(
            data=actual_routes_rdd,
            clustering_result=pickled_clustering_result,
            clustering_settings=clustering_settings,
            perfect_centroids=perfect_centroids,
        )

    best_setting = get_best_setting(metrics)
    print(best_setting)

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



def __run_all_tests():
    clustering_settings = {
        'clustering_algorithm': 'kmeans',
        'k_values': [8, 10, 12],
        'distance_function': route_distance,
        'max_iterations': 4,
        'debug_flag': False,
    }

    spark = None
    spark = SparkSession.builder.appName("Clustering").getOrCreate()


    print("Loading data")

    _ON_COLAB = 'google.colab' in sys.modules
    if clustering_settings['debug_flag']: print('On colab: ', _ON_COLAB)
    general_data_path = r'\data\data_12_06\1000_0.25_actual_routes.json'
    general_perfect_centroids_path = r'\data\data_12_06\10_standard_route.json'
    try:
        if _ON_COLAB:
            data_path = '/content/data_intensive_systems' + general_data_path.replace("\\", "/")
            perfect_centroid_path = '/content/data_intensive_systems/data/10_standard_route.json'
        else:
            data_path = os.getcwd() + general_data_path
            perfect_centroid_path = os.getcwd() + general_perfect_centroids_path
    except Exception as e:
        print('Data path was not found.\n\n', e)
        data_path = None
        perfect_centroid_path = None
    
    # Load datafile
    if clustering_settings['clustering_algorithm'] == 'kmodes':
        data = get_nested_data(spark, data_path, clustering_settings)
        perfect_centroids = get_nested_data(spark, perfect_centroid_path, clustering_settings)
    elif clustering_settings['clustering_algorithm'] == 'kmeans':
        data, indices2from_to_prods = get_vector_dataframe(spark, data_path, clustering_settings["debug_flag"])
        perfect_centroids = get_vector_dataframe(spark, perfect_centroid_path, clustering_settings["debug_flag"])
    else:
        data = None
        perfect_centroids = None
    
    # Run the clustering algorithm
    print("Running run_clustering().")
    results_and_metrics = run_clustering(
        data=data,
        clustering_settings=clustering_settings
    )
    
    # Save the results (optional, Abe)
    save_results_test(results_and_metrics, clustering_settings)

    # Evaluate the clustering
    results = results_and_metrics[0]
    metrics = results_and_metrics[1]

    if not metrics: # if kmodes, then metrics not yet calculated
        print("Start evaluating clusters")
        metrics = evaluate_clustering(
            data=data,
            clustering_result=results_and_metrics, 
            clustering_settings=clustering_settings
        )
        
    best_settings = get_best_setting(metrics)
    print("best settings are given by: \n", best_settings)

    return

def save_results_test(results, clustering_settings):
    os.makedirs('data/serialized_results_for_debugging/', exist_ok=True)
    # Make the name of the file
    algo = clustering_settings['clustering_algorithm']
    kvals = '[' + '-'.join( [str(kval) for kval in clustering_settings['k_values']] ) + ']'
    iters = clustering_settings['max_iterations']
    name = f"algo={algo}_kvalues={kvals}_max_iter={iters}"

    with open('data/serialized_results_for_debugging/results__{}__.pkl'.format(name), 'wb') as f:
        pickle.dump(results, f)

def load_results(path=r'data\serialized_results_for_debugging\results__algo=kmodes_kvalues=[3]_max_iter=4.pkl'):
    with open(path, 'rb') as f:
        return pickle.load(f)


if __name__ == "__main__":

    # res = evaluate_clustering_test2();    print('\n', res, '\n')

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
    
    # best_setting = get_best_setting(dummy_res); print(best_setting)

    __run_all_tests()
    
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
