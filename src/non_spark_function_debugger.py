# from parse_data import parse_json_data
from parse_data_3 import parse_json_data_3 # yo die oude gaf errors met importeren, maar idk of dit het doet

#pd_df, num_routes = parse_json_data(json_path="../data/10000_0.25_actual_routes.json")
pd_df, num_routes = parse_json_data_3(json_path="../data/10_standard_route.json")

print(pd_df)