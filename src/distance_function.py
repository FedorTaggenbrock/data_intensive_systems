from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
import numpy as np

def route_distance(route1, route2):
    intersection = np.logical_and(route1[1:], route2[1:]).sum()
    union = np.logical_or(route1[1:], route2[1:]).sum()
    return intersection / union

    #columns = route1.__fields__[1:]
    # intersection = 0
    # union = 0
    # for column in columns:
    #     if any(route1[column]) or any(route2[column]):
    #         union += 1
    #         if any(route1[column]) and any(route2[column]):
    #             intersection += 1
    # return float(intersection) / union if union != 0 else 0.0

