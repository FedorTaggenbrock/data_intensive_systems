import math
import numpy as np

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
                intersection += (1 - dictionary_distance(route1[column], route2[column]))
    if union != 0:
        dist = 1 - intersection / union
    else:
        dist = 1
    return dist

def test_distance_function(data):
    routes = data.collect()
    for i in range(len(routes)):
        for j in range(i, len(routes)):
            print("Distance between route ", i, " and ", j, " is given by: ", route_distance(routes[i], routes[j]))