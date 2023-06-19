import json
import pandas as pd
from pyspark.sql.functions import collect_list, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, IntegerType, StringType


import json
import ijson
import pandas as pd
from pyspark.sql.functions import collect_list, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, IntegerType, StringType, FloatType
import warnings
import math
from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType, ArrayType

from pyspark.ml.linalg import SparseVector, VectorUDT



def parse_nested_data(json_path, debug_flag=False):
    """
    Parse the data from the json file to a pandas df.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)
    num_routes = 0
    from_tos = set()
    products = set()
    with open(json_path, 'rb') as f:
        for row in ijson.items(f, "item"):
            num_routes += 1
            for trip in row['route']:
              from_to = trip['from']+"-"+trip['to']
              from_tos.add(from_to) #does not add duplicate from_to's
              for prod in trip["merchandise"]:
                products.add(prod)
    from_tos = list(from_tos)
    products = list(products)
    #Integers relate to specific products.
    #For bookkeeping we store a dictionary which store the index of a specific product.
    product_mapping = {}
    for i,v in enumerate(products):
      product_mapping[v] = i

    df_rows = []
    with open(json_path, 'rb') as f:
      for row in ijson.items(f, "item"):
        new_row = {"id-sr": str(row["id"])+" "+ str(row["sr"])}
        for trip in row['route']:
            from_to = trip['from']+"-"+trip['to']
            merch_dict = dict(map(lambda x: (product_mapping[x[0]], x[1]), trip["merchandise"].items()))
            trip_merch = {from_to: merch_dict}
            new_row.update(trip_merch)
        df_rows.append(new_row)
    df = pd.DataFrame(df_rows, columns=["id-sr"] +from_tos)
    df = df.applymap(lambda x: {} if pd.isna(x) else x)
    return df, from_tos, products, num_routes


def get_vector_dataframe(spark, json_path, debug_flag=False):
  """
  Functions that reads in the row's of the data and stores all the features in a sparse vector (similar to a dict). A feature of a route is defined by (from city, to city, product).
  Every feature is represented by an integer key and the value is the amount of product.
  A row in the resulting dataframe has the following structure:
  Row(id-sr='0 0', features=SparseVector(38466, {237: 10.0, 4952: 14.0, 6185: 6.0, 9978: 15.0, 10914: 19.0, 11111: 9.0, 11429: 6.0, 12992: 10.0, 13258: 7.0, 15131: 8.0, 15287: 9.0, 17500: 7.0, 18674: 6.0, 18898: 19.0, 19172: 17.0, 20302: 13.0, 20366: 11.0, 20738: 6.0, 21733: 7.0, 23069: 11.0, 23793: 7.0, 26398: 6.0, 26739: 12.0, 27197: 13.0, 29726: 7.0, 30636: 16.0, 32340: 17.0, 33394: 7.0, 33651: 19.0, 33868: 7.0, 33972: 12.0}))
  The function also returns a dict that translates the sparse vector keys back as follows: 237: 'city_234-city_130-product_136'
  """
  num_routes = 0
  from_to_prods = set()
  with open(json_path, 'rb') as f:
      for row in ijson.items(f, "item"):
          num_routes += 1
          for trip in row['route']:
            for prod in trip["merchandise"]:
              from_to_prods.add(trip['from']+"-"+trip['to']+"-" + prod)
  from_to_prods = list(from_to_prods)
  indices2from_to_prods = {idx: ft_prod for idx,
                           ft_prod in enumerate(from_to_prods)}
  from_to_prods2indices = {ft_prod: idx for idx,
                           ft_prod in enumerate(from_to_prods)}

  sparse_vectors = []
  with open(json_path, 'rb') as f:
      for row in ijson.items(f, "item"):
          route_sparse_dict = {}
          for trip in row['route']:
              for prod, val in trip['merchandise'].items():
                  from_to_prod = from_to_prods2indices[trip['from'] +
                                                       "-" + trip['to'] + "-" + prod]
                  if from_to_prod in route_sparse_dict:
                      route_sparse_dict[from_to_prod] += val
                  else:
                      route_sparse_dict[from_to_prod] = val
          sparse_vectors.append(
              (str(row['id']) + " " + str(row['sr']), dict(sorted(route_sparse_dict.items()))))

  sparse_vectors_with_indices = [
      (id_sr, SparseVector(len(from_to_prods), list(
          route_dict.keys()), list(route_dict.values())))
      for id_sr, route_dict in sparse_vectors
  ]

  schema = StructType([StructField("id-sr", StringType()),
                       StructField("features", VectorUDT())])
  spark_df = spark.createDataFrame(sparse_vectors_with_indices, schema=schema)

  if debug_flag:
      print("Dataset structure")
      spark_df.show(5)
      print("What a single element looks like:")
      print(spark_df.head())
      print("indices 2 from_to_prod:", indices2from_to_prods)
  return spark_df, indices2from_to_prods

def get_nested_data(spark, path, clustering_settings):
  """
  Reads in the data in a more human readable format. Every route has multiple columns, each representing a trip. The value for every trip is a (sparse) dictionary containing the products for that trip.
  These routes fit into the custom created distance function which might be a good way to assign costs to routes depending on how much they differ from their cluster centres.
  """
  df, from_tos, products, num_routes = parse_nested_data(path, clustering_settings["debug_flag"])
  schema = StructType([StructField("id-sr",StringType())] + [StructField(from_to, MapType(IntegerType(), IntegerType())) for from_to in from_tos]  )
  spark_df = spark.createDataFrame(df, schema = schema)
  if clustering_settings["debug_flag"]:
    spark_df.show()
  clustering_settings["num_routes"] = num_routes
  return spark_df.rdd


def get_data(spark, path, clustering_settings: dict):
   # Dispatcher to get right data
    if clustering_settings['clustering_algorithm'] == 'kmodes':
        return get_nested_data(spark, path, clustering_settings)
    elif clustering_settings['clustering_algorithm'] == 'kmeans':
        return get_vector_dataframe(spark, path, clustering_settings["debug_flag"])
    else:
        print("Clustering algorithm not supported or misspelled: 'kmodes' or 'kmeans'")
        return None




