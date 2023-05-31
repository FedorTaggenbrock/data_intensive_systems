import json
import pandas as pd
import inspect
from pyspark.sql.functions import collect_list
from pyspark.sql import SparkSession
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
    - encode_style: Indicates in what style to generate the data. Used to e.g. only keep categorical features; mostly for testing.
                        Possible values: 'all', 'num_only', 'cat_only'.
    - one_hot_encode: bool indicating whether to one-hot encode the cities or not.
    
    Returns:
    - pandas DataFrame containing the encoded data.
    """
    print(df)
    print("start df encoding")

    # Create a new column with the combined 'from' and 'to' values
    df['from_to'] = df['from'] + '-' + df['to']
    df = df.drop(columns = ['from', 'to'])
    print("df1", df)

    # Get a list of all columns except for route_id, from, to, from_to, and to_from
    product_cols = [col for col in df.columns if col not in ['route_id', 'from_to']]

    print("product_cols", product_cols)

    # Melt the product columns into a single column
    melted_df = df.melt(id_vars=['route_id', 'from_to'], value_vars=product_cols, var_name='product',
                        value_name='value')

    print("melted_df")
    print(melted_df)

    # Pivot the table to have one row per route_id and columns for each from_to and to_from combination
    result = pd.pivot_table(melted_df, index=['route_id', 'product'], columns=['from_to'], values='value',
                            aggfunc='first')
    product_list = result["product"]
    print(product_list)

    # Reset the index and fill NaN values with 0
    result = result.reset_index().fillna(0)

    print("pd result")
    print(result)

    spark_df = spark.createDataFrame(result)
    from_to_cols = [col for col in spark_df.columns if col not in ['route_id', 'product']]
    spark_df = spark_df.groupBy("route_id").agg(*[collect_list(c).alias(c) for c in from_to_cols])

    print("encoded spark df")
    spark_df.show(truncate=False)

    return spark_df

    # if encode_style == 'all':
    #     if one_hot_encode:
    #         # One-hot encode the cities
    #         df = pd.get_dummies(df, columns=['from', 'to'])
    #
    #     # Scale the one-hot features appropriately;
    #     # To do this, first select all one-hot columns.
    #     # one_hot_cols = [col for col in df.columns if col.startswith('from_') or col.startswith('to_')]
    #
    #
    # elif encode_style == 'num_only':
    #     # Drop categorical columns
    #     df.drop(columns=['from', 'to'], inplace=True)
    # elif encode_style == 'cat_only':
    #     # Drop numerical columns (i.e. keep only the categorical columns)
    #     df.drop(columns=[col for col in df.columns if col not in ['from', 'to']], inplace=True)
    #     if one_hot_encode:
    #         # One-hot encode the cities
    #         df = pd.get_dummies(df, columns=['from', 'to'])
    # else:
    #     raise NotImplementedError(f'encode_style "{encode_style}" not implemented in function "{inspect.currentframe().f_code.co_name}."')




def parse_test():
    # Write comment that says this is only used for testing
    df = parse_json_data()
    df = encode_data(df, encode_style='all_')

    # Print formatting
    pd.set_option('expand_frame_repr', True)
    pd.set_option('display.width', 100)
    pd.set_option('display.max_columns', 100)

    print(df.head())

if __name__ == '__main__':
    parse_test()