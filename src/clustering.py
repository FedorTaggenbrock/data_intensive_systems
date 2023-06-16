from copy import copy
from statistics import mode
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from functools import reduce

import numpy as np
import math

# import evaluate_clustering


from copy import copy
from statistics import mode
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from functools import reduce

import numpy as np
import math
from collections import Counter
import random


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
            if clustering_settings["debug_flag"]:
                print("The centroids for  k = ", current_k, " are given by: ", [c[0] for c in predicted_centroids] )
    else:
        print("Clustering algorithm setting not recognized in run_and_tune().")
    if clustering_settings["debug_flag"]:
        print("The output results for multiple k is given by:", results)
    return results



def route_distance(route1, route2):
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

    columns = route1.__fields__[1:]
    intersection = 0
    union = 0
    intersecting_dist = 0
    # Preferably vectorize this, something with zip?
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


def kModes(data: RDD, k: int, clustering_settings):
    
    def assign_row_to_centroid_key(row, centroids):
      random_centroid = random.choice(centroids)
      min_centroid = min(centroids, key=lambda centroid: route_distance(row, centroid))
      if route_distance(row, random_centroid) == route_distance(row, min_centroid):
          return (random_centroid["id-sr"], row)
      else:
          return (min_centroid["id-sr"], row)

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
        max_score = -1
        for it, row in enumerate(set_of_rows):
            if row_scores[it] > max_score:
                best_row = row
                max_score = row_scores[it]
            if row_scores[it] == max_score:
              if random.random() <= 0.2:
                best_row = row

        # if clustering_settings["debug_flag"]:
        #   print("trips_to_keep", [int(bl) for bl in trips_to_keep])
        #   print("row_scores", row_scores)

        return best_row

    centroids = data.takeSample(withReplacement=False, num=k)
    if clustering_settings["debug_flag"]:
        print("centroids = ",  [c[0] for c in centroids])

    # Iterate until convergence or until the maximum number of iterations is reached
    for i in range(clustering_settings["max_iterations"]):
        if clustering_settings["debug_flag"]:
          print("iteration ", i, ": ")
        # Assign each point to the closest centroid
        clusters = data.map(lambda row: assign_row_to_centroid_key(row, centroids)).groupByKey()

        if clustering_settings["debug_flag"]:
          print("Mapped rows to existing centroids")
          ls_set_of_rows = list(clusters.take(k))
          for i in range(len(ls_set_of_rows)):
            print("number of rows in the", i, "-th cluster per st route:" , Counter([row_[0].split()[1] for row_ in ls_set_of_rows[i][1]]) )
          print("Computing the new centroid for the first cluster:")
          print("new_c= ", create_centroid(ls_set_of_rows[0][1]))

        centroids = clusters.map(lambda key_rows: create_centroid(key_rows[1])).collect()

        if clustering_settings["debug_flag"]:
            print("centroids = ",  [c[0] for c in centroids])

    return [list(x) for x in centroids]

def run_all_tests():
    clustering_settings = {
        'clustering_algorithm': 'kmodes',
        'k_values': [10],
        'max_iterations': 5,
        'debug_flag': False
    }

    spark = SparkSession.builder.appName("Clustering").getOrCreate()

    # actual_routes_rdd, num_routes = get_data(spark, 'data_intensive_systems/data/1000_0.25_actual_routes.json', clustering_settings)
    actual_routes_rdd, num_routes = parse_data_3.get_data_3(spark, 'data_intensive_systems/data/1000_0.25_actual_routes.json', clustering_settings)

    clustering_settings["num_actual_routes"] = num_routes

    print("Running run_clustering().")
    results = run_clustering(
        data=actual_routes_rdd,
        clustering_settings=clustering_settings
        )

    print("Start evaluating clusters")
    metrics = evaluate_clustering.evaluate_clustering(actual_routes_rdd, results, clustering_settings)
    best_settings = evaluate_clustering.get_best_setting(metrics)
    print("best settings are given by: \n", best_settings)
    
    return

