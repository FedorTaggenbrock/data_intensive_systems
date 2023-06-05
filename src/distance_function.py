
import numpy as np
import cmath as math
def dictionary_distance(dict1, dict2):
    #This function computes the euclidean distance for dict representations of (sparse) vectors.
    #The get method is used to return a default value of 0 for keys that are not present in one of the dictionaries
    return math.sqrt(np.sum([(int(float(dict1.get(product, 0))) - int(float(dict2.get(product, 0)))) ** 2 for product in set(dict1) | set(dict2)]))

def route_distance(route1, route2):
    columns = route1.__fields__[1:]
    intersection = 0
    union = 0
    for column in columns:
        print(route1[column])
        trip1 = any(route1[column])
        trip2 = any(route2[column])
        if trip1 or trip2:
            union += 1
            if trip1 and trip2:
                intersection += dictionary_distance(route1[column], route2[column])
    return float(intersection) / union if union != 0 else 0.0


