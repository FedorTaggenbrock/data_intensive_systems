from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

def route_distance(route1, route2):
    columns = route1.schema.names
    intersection = 0
    union = 0
    for column in columns:
        if any(route1[column]) or any(route2[column]):
            union += 1
            if any(route1[column]) and any(route2[column]):
                intersection += 1
    return float(intersection) / union if union != 0 else 0.0


def jaccard_distance(a, b):
    a = np.array(a)
    b = np.array(b)
    intersection = np.sum(a & b)
    union = np.sum(a | b)
    return 1 - (intersection / union)