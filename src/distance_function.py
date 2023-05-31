import numpy as np

def route_distance():
    return

def jaccard_distance(a, b):
    a = np.array(a)
    b = np.array(b)
    intersection = np.sum(a & b)
    union = np.sum(a | b)
    return 1 - (intersection / union)