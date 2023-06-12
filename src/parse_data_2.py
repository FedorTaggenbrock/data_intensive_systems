import json
import pandas as pd
from pyspark.sql.functions import collect_list, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, IntegerType, StringType


def parse_json_data2(json_path='../data/data_12_06/10_standard_route.json', debug_flag=False):
    """Parse the data from the json file to a pandas df."""

    # Load the json data from file into string
    with open(json_path, 'r') as f:
        json_as_str = json.load(f)  # use json.load() instead of json.loads()

    # Initialize an empty list to hold the route dicts
    df_list = []
    # Iterate over each dict in the data; each dict represents one full route (can include multiples stops)
    for d in json_as_str:
        trip_dict = {}
        # Iterate over each trip in the route
        trip_id = 0
        for trip in d['route']:
            trip_dict[trip_id] = {'from': trip['from'], 'to': trip['to'], 'merchandise' : trip['merchandise']}
            trip_id += 1
        df_list.append({'route_id': d['id'], 'sr': d['sr'], 'trips' : trip_dict})


    df = pd.DataFrame(df_list)
    return df

def encode_data2(spark: SparkSession, df: pd.DataFrame):
    new_data = []

    # iterate over the rows in your DataFrame
    for idx, row in df.iterrows():
        # iterate over the trips in each row
        for trip_id, trip_data in row['trips'].items():
            # make a copy of the row data
            new_row = row.to_dict()
            # add trip_id to the data
            new_row['trip_id'] = trip_id
            # add 'from', 'to' to the data
            new_row['from'] = trip_data['from']
            new_row['to'] = trip_data['to']
            # add merchandise items to the data
            for product_id, quantity in trip_data['merchandise'].items():
                new_row[product_id] = quantity
            # append the new row to the new_data list
            new_data.append(new_row)

    # create a new DataFrame from new_data
    new_df = pd.DataFrame(new_data)

    # replace NaNs with 0
    new_df.fillna(0, inplace=True)

    # create a Spark DataFrame from your new pandas DataFrame
    spark_df = spark.createDataFrame(new_df)
    spark_df.show()


def encode_data(spark: SparkSession, df: pd.DataFrame, debug_flag=False):
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
        original_spark_df.show(truncate=False)

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

    product_list = result.loc[:, "product"]

    spark_df = spark.createDataFrame(result)

    def list_to_dict(ls):
        return {index: value for index, value in enumerate(ls) if value > 0}

    # Combine all the rows with the same route_id in such a way that all the different values per column are combined into a list.
    from_to_cols = [col for col in spark_df.columns if col not in ['route_id', 'product']]
    list_to_dict_udf = udf(list_to_dict, MapType(IntegerType(), StringType()))
    spark_df = spark_df.groupBy("route_id").agg(*[list_to_dict_udf(collect_list(c)).alias(c) for c in from_to_cols])

    if debug_flag:
        print("Spark dataframe after encoding:")
        spark_df.show(truncate=False)
        print("product list:")
        print(product_list)
        print("elements 18, 4, 12 from product_list:", product_list[18], product_list[4], product_list[12])

    return spark_df, product_list

