from parse_data import parse_json_data

#pd_df, num_routes = parse_json_data(json_path="../data/10000_0.25_actual_routes.json")
pd_df, num_routes = parse_json_data(json_path="../data/10_standard_route.json")

print(pd_df)