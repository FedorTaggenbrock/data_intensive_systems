from parse_data import parse_json_data
# from parse_data_3 import parse_json_data_3

def main():
    #pd_df, num_routes = parse_json_data(json_path="../data/10000_0.25_actual_routes.json")
    pd_df, num_routes = parse_json_data(json_path="../data/10_standard_route.json") 
    '''yo dit path doet het niet, je moet checken of je op colab zit, en als dat zo is ../data aan je path appenden (er vanuitgaande dat die file daar dan is)'''

    print(pd_df)

if __name__ == "__main__":
    main()