import math

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
import numpy as np
from scipy.sparse import csr_matrix
from scipy.spatial.distance import cdist

def route_distance(route1, route2):
    columns = route1.__fields__[1:]
    intersection = 0
    union = 0
    for column in columns:
        if any(route1[column]) or any(route2[column]):
            union += 1
            if any(route1[column]) and any(route2[column]):
                intersection += dictionary_distance(route1[column], route2[column])
    return float(intersection) / union if union != 0 else 0.0

def dictionary_distance(dict1, dict2):
    #This function computes the euclidean distance for dict representations of (sparse) vectors.
    #The get method is used to return a default value of 0 for keys that are not present in one of the dictionaries
    return math.sqrt(np.sum( [ (int(dict1.get(product, 0)) - int(dict2.get(product, 0))) ** 2 for product in set(dict1) | set(dict2)] ))