from copy import copy
from statistics import mode
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from functools import reduce

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
            print("Performing clustering with k= ", current_k)
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
    # Painfull code duplication which is the only way I managed to make all the spark dependencies work
    def dictionary_distance(dict1, dict2):
        # This function computes the normalized euclidean distance (in 0-1) for dict representations of (sparse) vectors.
        norm_dict1 = math.sqrt(np.sum(
            [int(float(v)) ** 2 for k, v in dict1.items()]))
        norm_dict2 = math.sqrt(np.sum(
            [int(float(v)) ** 2 for k, v in dict2.items()]))
        return math.sqrt(np.sum(
            [(int(float(dict1.get(product, 0))) - int(float(dict2.get(product, 0)))) ** 2 for product in
             set(dict1) | set(dict2)])) / (norm_dict1 + norm_dict2)

    def route_distance(route1, route2):
        columns = route1.__fields__[1:]
        intersection = 0
        union = 0
        intersecting_dist = 0
        # Preferably vectorize this
        for column in columns:
            trip1 = route1[column]
            trip2 = route2[column]
            if trip1 or trip2:
                union += 1
                if trip1 and trip2:
                    intersection += (1 - dictionary_distance(trip1, trip2))
        if union != 0:
            dist = 1 - intersection / union
        else:
            dist = 1
        return dist

    def assign_row_to_centroid_key(row, centroids):
        best_centroid = min(centroids, key=lambda centroid: route_distance(row, centroid))
        return (best_centroid["route_id"], row)

    def create_centroid(set_of_rows):
        size_of_set = len(set_of_rows)
        trips_to_keep = []
        first_row = True
        for row in set_of_rows:
            if first_row:
                trips_to_keep = np.zeros(len(row))
                first_row = False
            for it, trip in enumerate(row):
                if trip:
                    trips_to_keep[it] += 1
        trips_to_keep = trips_to_keep >= size_of_set // 2
        row_scores = []
        for row in set_of_rows:
            row_score = 0
            for it, trip in enumerate(row):
                if it != 0 and trip and trips_to_keep[it]:
                    row_score += 1
            row_scores.append(row_score)
        max_score = 0
        for it, row in enumerate(set_of_rows):
            if row_scores[it] > max_score:
                best_row = row
                max_score = row_scores[it]
        return best_row

    centroids = data.takeSample(withReplacement=False, num=k)
    if clustering_settings["debug_flag"]:
        print("centroids = ", centroids)

    # Iterate until convergence or until the maximum number of iterations is reached
    for i in range(clustering_settings["max_iterations"]):
        # Assign each point to the closest centroid
        clusters = data.map(lambda row: assign_row_to_centroid_key(row, centroids)).groupByKey()

        centroids = clusters.map(lambda key_rows: create_centroid(key_rows[1])).collect()

        if clustering_settings["debug_flag"]:
            print("clusters = ", clusters.collect())
            print("centroids = ", centroids)

    return [list(x) for x in centroids]
