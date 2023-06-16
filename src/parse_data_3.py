import json
import ijson
import pandas as pd
from pyspark.sql.functions import collect_list, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, IntegerType, StringType
import warnings
import math
from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType, ArrayType


def parse_json_data_3(json_path, debug_flag=False):
    """Parse the data from the json file to a pandas df."""
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

    if debug_flag:
       print("product_mapping: ", product_mapping)

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

def get_data_3(spark, path, clustering_settings):
  df, from_tos, products, num_routes = parse_json_data_3(path, clustering_settings["debug_flag"])
  
  schema = StructType([StructField("id-sr",StringType())] + [StructField(from_to, MapType(IntegerType(), IntegerType())) for from_to in from_tos]  )
  spark_df = spark.createDataFrame(df, schema = schema)
  if clustering_settings["debug_flag"]:
    spark_df.show()
  clustering_settings["Products"] = products
  clustering_settings["num_routes"] = num_routes

  return spark_df.rdd, num_routes



