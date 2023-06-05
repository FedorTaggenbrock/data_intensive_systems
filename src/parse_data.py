import json
import pandas as pd
import inspect
from pyspark.sql.functions import collect_list, udf, broadcast, lit
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, IntegerType, StringType


def parse_json_data(json_path='data_intensive_systems/data/ex_example_route.json', debug_flag = False):
    """Parse the data from the json file to a pandas df."""

    # Load the json data from file into string
    with open(json_path, 'r') as f:
        json_as_str = json.load(f)   # use json.load() instead of json.loads()

    # Initialize an empty list to hold the route dicts
    df_list = []

    num_routes = 0
    # Iterate over each dict in the data; each dict represents one full route (can include multiples stops)
    for d in json_as_str:
        route_id = d['id']
        num_routes+=1
        # Iterate over each trip in the route
        for trip in d['route']:
            # Store the cities the trip is between
            from_city = trip['from']
            to_city = trip['to']
            # Collect all types of merchandise
            merchandise_dict = trip['merchandise']
            # Update dict with the non-merchandise info
            merchandise_dict.update({'route_id': route_id, 'from': from_city, 'to': to_city})

            # Add a dictionary to df_list representing a row in the future df
            df_list.append(merchandise_dict)
    
    # Finally, create df from the list. Fill NaNs with 0. Needed because if the last datapoint
    # contains an unseen merchandise, that column for all the previous routes will be filled with NaN.
    df = pd.DataFrame(df_list)
    df.fillna(0, inplace=True)

    # Reorder df cols to have route_id,to,from, as the first columns
    cols = ['route_id', 'from', 'to'] + [col for col in df if col not in ['route_id', 'from', 'to']]
    df = df[cols]
    return df, num_routes

def encode_data(spark: SparkSession, df: pd.DataFrame, debug_flag =False):
    """
    Encode data to be used for clustering. One-hot encode the cities.
    
    Args:
    - df: pandas DataFrame containing the data to be encoded
    Returns:
    - Spark DataFrame containing the encoded data.
    """
    if debug_flag:
        original_spark_df = spark.createDataFrame(df)
        print("spark dataframe before encoding")
        original_spark_df.show()

    # Create a new column with the combined 'from' and 'to' values and remove the original from, to columns.
    df['from_to'] = df['from'] + '-' + df['to']
    df = df.drop(columns = ['from', 'to'])

    # Get a list of all columns except for route_id and from_to
    product_cols = [col for col in df.columns if col not in ['route_id', 'from_to']]

    # Melt the product columns into a single column
    melted_df = df.melt(id_vars=['route_id', 'from_to'], value_vars=product_cols, var_name='product',
                        value_name='value')

    # Pivot the table to have one row per route_id and columns for each from_to combination
    result = pd.pivot_table(melted_df, index=['route_id', 'product'], columns=['from_to'], values='value',
                            aggfunc='first')
    # Reset the index and fill NaN values with 0
    result = result.reset_index().fillna(0)

    product_list = result.loc[:,"product"]

    spark_df = spark.createDataFrame(result)

    def list_to_dict(ls, product_list):
        return {product_list[index]: value for index, value in enumerate(ls)}
    #Combine all the rows with the same route_id in such a way that all the different values per column are combined into a list.
    from_to_cols = [col for col in spark_df.columns if col not in ['route_id', 'product']]
    list_to_dict_udf = udf(list_to_dict, MapType(IntegerType(), StringType()))
    spark_df = spark_df.groupBy("route_id").agg(*[list_to_dict_udf(collect_list(c), product_list).alias(c) for c in from_to_cols])

    if debug_flag:
        print("Spark dataframe after encoding:")
        spark_df.show(truncate=False)
        # print("Intermediate pandas dataframe")
        # print(result)
        print("product list:")
        print(product_list)

    return spark_df, product_list

