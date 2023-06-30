"""

File which generates the dataset and save it in the folder called "data"

"""

import random
import json
import os
import copy
from threading import Thread
import threading
import time

def generate_data():

    # 250 cities
    cities = ["city_{}".format(i) for i in range(250)] # Lol gast
    # cities = ["city_0", "city_1", "city_2", "city_3", "city_4"]

    # 1000 products
    products = ["product_{}".format(i) for i in range(1000)]
    # products = ["product_0", "product_1", "product_2", "product_3", ...

    class Product:
        def __init__(self, name: str, count: int, range: range):
            self.name  = name
            self.count = count
            self.range = range

        def __str__(self) -> str:
            return '"' + str(self.name) + '":' + str(self.count)
        
        def permute(self, probability, verbose = False):
            """ This function permuts the amount of a product given some percentage """
            r = random.random()

            if r < probability:
                self.count = random.choice(self.range)

            # if verbose: print(f"[Product permutation]: {str(self)}")
        
    class Stop:
        def __init__(
                self, origin: str, destination:str,
                city_list: list, product_list: list, 
                product_amount_range: range
            ):

            self.origin = origin
            self.destination = destination
            self.products = []

            # maintain data lists for data generation on permutation
            self.city_list = city_list
            self.product_list = product_list
            self.product_amount_range = product_amount_range

        def __str__(self) -> str:
            s =  '{"from":"' + self.origin + '","to":"' + self.destination + '","merchandise":{'

            for p in self.products:
                s += str(p) + ","
        
            if len(self.products) == 0: s += "}},"
            else: s = s[:-1] + "}},"
            
            return s

        def generate_products(self, product_range):
            """ This function generates a list of products for a given Stop """
            p = int(random.uniform(product_range.start, product_range.stop))

            while len(self.products) <= p:
                c = int(random.uniform(self.product_amount_range.start, self.product_amount_range.stop))
                i = random.choice(self.product_list)

                if self.product_exists(i):
                    continue

                self.products.append( Product(i, c, self.product_amount_range) )

        def product_exists(self, product_name: str) -> bool:
            """ returns true if 'product_name' already exists """
            for product in self.products:
                if product_name == product.name: 
                    return True
                
            return False

        def permute(self, probability, verbose = False):
            """ 
                This function permutes a stop, this can be done in 3 ways 
                1. P( add product     ) = 1/3 
                2. P( remove product  ) = 1/3
                3. P( permute product ) = 1/3
            """
            r = random.random()

            if r < probability / 2:
                """ add an unique product to the productlist """
                unique = False
                while not unique:
                    name   = random.choice(self.product_list)
                    if not self.product_exists(name):
                        unique = True

                amount = random.choice(self.product_amount_range)
                product = Product(name, amount, self.product_amount_range)

                self.products.append( product )

                # if verbose: print(f"[Stop permutation 1]: added {str(product)}")

            if probability / 2 <= r < probability:
                """ remove a random product in the list """
                if len(self.products) > 2:
                    product = random.choice(self.products)
                    self.products.remove(product)

                # if verbose: print(f"[Stop permutation 2]: removed {str(product)}")

            # permute all products based on the probability factor
            for product in self.products:
                product.permute(probability, verbose)

    class Route:
        stops = []

        def __init__(self, id, city_list: list, prodcut_list: list, product_count: range, product_range: range):
            self.id = id
            self.sr = -1
            self.stops = []
            self.city_list = city_list
            self.product_list = prodcut_list
            self.product_count = product_count  # amount of product
            self.product_range = product_range  # amount per product

        def __str__(self) -> str:
            s = '{"id":' + str(self.id) + ', "sr": '+str(self.sr)+', "route":['
            for x in self.stops:
                s += str(x)
            return s[:-1] + ']}'
        
        def generate_stops(self, city_count: range):
            """ This function generates a list of Stops in for a Standard Route """

            s = random.choice(city_count)

            while (len(self.stops) <= s):
                if (len(self.stops) == 0):  stop_1 = random.choice(self.city_list)
                else:                       stop_1 = self.stops[len(self.stops) - 1].destination
                stop_2 = random.choice(self.city_list)

                if (stop_1 == stop_2):
                    continue
                

                stop = Stop(stop_1, stop_2, self.city_list, self.product_list, self.product_count)
                stop.generate_products(self.product_range)

                self.stops.append( stop )

        def permute(self, probability, verbose = False):
            """ 
                This function permutes a route, this can be done in 3 ways 
                1. P( add stop     ) = percentage 
                2. P( remove stop  ) = percentage
                3. P( permute stop ) = percentage
            """
            r = random.random()

            # TODO: change probility intervals

            if r < probability / 2:
                """ add a stop to the stops list """
                # random position to insert
                idx = self.stops.index(random.choice(self.stops))

                # only performe linked-list add operation if not start or end
                if idx != 0 and idx != len(self.stops) - 1:
                    # idx = between (start, end)
                    # [a,b], [b,c] + [x] => [a,b], [b,x], [x,c]
                    prev_stop = self.stops[idx - 1]
                    next_stop = self.stops[idx + 1]
                    new_stop  = random.choice(self.city_list)

                    stop = Stop(prev_stop.destination, new_stop, self.city_list, self.product_list, self.product_count)
                    stop.generate_products(self.product_range)
                    next_stop.origin = stop.destination

                    if verbose: print(f"[Route permutation 1]: added {str(stop)} at {idx}, previous:{prev_stop}, next:{next_stop}")

                elif idx == 0:
                    # idx = start
                    next_stop = self.stops[0]
                    stop = Stop(random.choice(self.city_list), next_stop.origin, self.city_list, self.product_list, self.product_count )
                    self.stops.insert(0, stop)

                    if verbose: print(f"[Route permutation 1]: added {str(stop)} at {idx}, next {next_stop}")

                else:
                    # idx = end
                    prev_stop = self.stops[len(self.stops) - 1]
                    stop = Stop(prev_stop.destination, random.choice(self.city_list), self.city_list, self.product_list, self.product_count)
                    self.stops.append(stop)

                    if verbose: print(f"[Route permutation 1]: added {str(stop)} at {idx}, previous {prev_stop}")
                
            if probability / 2 <= r < probability:
                """ remove a random stop in the list """
                stop = random.choice(self.stops)
                idx  = self.stops.index(stop)

                # only perform linked-list remove operation if not start or end
                if idx != 0 and idx != len(self.stops) - 1:
                    # [a,b], [b,c], [c,d] => [a,c], [c,d]
                    prev_stop = self.stops[idx - 1]
                    next_stop = self.stops[idx + 1]
                    prev_stop.destination = next_stop.origin
                
                # remove the stop from the list
                self.stops.remove(stop)

                if verbose: print(f"[Route permutation 2]: removed {str(stop)} at {idx}")

            # permute all stops based on the probability factor
            for stop in self.stops:
                stop.permute(probability, verbose)

        def pretty_print(self):
            print(json.dumps(json.loads(str(self)), indent=1))

        @staticmethod
        def generate_routes(
                stop_count: int, 
                city_count: range, 
                product_range: range, 
                product_count: range, 
                city_list: list, 
                product_list: list
            ):
            """ This function can generate a list of Standard Routes """

            routes = []

            for i in range(stop_count): 
                r = Route(i, city_list, product_list, product_count, product_range)
                r.generate_stops(city_count)
                routes.append( r )

            return routes

        @staticmethod
        def scramble_routes(routes, iterations):
            for a in range(iterations):
                route_1 = random.choice(routes)
                route_2 = random.choice(routes)

                stop_route_1 = random.choice(route_1.stops)
                stop_route_2 = random.choice(route_2.stops)

                # count = max(len(stop_route_2.products), len(stop_route_1.products))
                #
                # n = random.choice(range(count))
                #
                # for b in range(n):
                #     product_1 = random.choice(stop_route_1.products)
                #     product_2 = random.choice(stop_route_2.products)
                #
                #     stop_route_1.products.remove(product_1)
                #     stop_route_1.products.append(product_2)
                #
                #     stop_route_2.products.append(product_1)
                #     stop_route_2.products.remove(product_2)

                # route_1.stops.remove(stop_route_1)
                route_1.stops.append(stop_route_2)

                # route_2.stops.remove(stop_route_2)
                route_2.stops.append(stop_route_1)

                print("scrambled")

            return routes

        @staticmethod
        def write_routes_json(routes: list) -> str:
            # s = "["
            # for r in routes:
            #     s += (str(r) + ",")
            # s = s[:-1] + ']'
            # return s
            s = ""
            for r in routes:
                s += (str(r) + ",")
            s = s[:-1] + ','
            return s
        
        @staticmethod
        def read_routes_json(filename: str, mutate_product_range: range, mutate_product_count: range, product_list: list, city_list: list) -> list:
            f = open(filename)

            data = json.load(f)
            routes = []

            for route_ in data:
                id = route_['id']
                stops = []

                for stop_ in route_['route']:
                    from_ = stop_['from']
                    to_   = stop_['to']
                    products_ = []

                    for product_ in stop_['merchandise']:
                        product = Product(product_, stop_['merchandise'][product_], mutate_product_count)
                        products_.append(product)

                    stop = Stop(from_, to_, cities, product_list, mutate_product_range)
                    stop.products = products_
                    stops.append(stop)

                route = Route(id, city_list, product_list, mutate_product_count, mutate_product_range)
                route.stops = stops
                routes.append(route)

            return routes


    product_range_ = range(5, 15)
    product_count_ = range(1, 100)

    # routes = Route.generate_routes(
    #     25,
    #     city_count=range(5, 15),
    #     product_range=product_range_,
    #     product_count=product_count_,
    #     city_list=cities,
    #     product_list=products,
    # )

    instance = 'bigger'

    routes = Route.read_routes_json(
        f"../25_{instance}_standard_route.json",
        mutate_product_range=product_range_,
        mutate_product_count=product_count_,
        product_list=products,
        city_list=cities
    )

    # routes_ = Route.scramble_routes(routes, 5)
    #
    # f = open(f"../{len(routes)}_baseline_scrambled_standard_route.json", "w")
    # f.write(Route.write_routes_json(routes_))
    # f.close()

    ar = 25
    max_size = 50000

    n = int(max_size / ar)
    permutation = 0.75

    def permutate_route(id, route, p):
        c = 0
        id_ = 0

        permuted = []

        for i in range(n):
            route_ = copy.deepcopy(route)

            route_.permute(p)
            route_.sr = route.id
            route_.id = id_
            permuted.append(route_)
            id_ += 1

            interval = 2.5
            if (100/n*i) % interval == 1:
                c+=1
                print(f"Thread {id}: {c*interval}%")
                # f = open(f"sr={len(routes)}_ar={n}_p={permutation}_actual_routes.json", "a")
                # f.write(Route.write_routes_json(permuted))
                # f.close()
                # permuted = []

        f = open(f"p={permutation}_{instance}_actual_routes.json", "a")
        f.write(Route.write_routes_json(permuted))
        f.close()

    st = time.time()

    threads = []
    f = open(f"p={permutation}_{instance}_actual_routes.json", "w")
    f.write('[')
    f.close()

    for route in routes:
        print(f"Creating thread {route.id}")
        thread = Thread(target= permutate_route, args=[route.id, route, permutation])
        threads.append(thread)
        thread.start()

    while threading.active_count() > 1:
        for t in threads:
            t.join()
            print(f"{t}, is done.")
    print("Done permutating")

    f = open(f"p={permutation}_{instance}_actual_routes.json", "rb+")
    f.seek(-1, os.SEEK_END)
    f.truncate()
    f.close()

    f = open(f"p={permutation}_{instance}_actual_routes.json", "a")
    f.write(']')
    f.close()

    # f = open(f"{len(routes)}_final_standard_route.json", "w")
    # f.write(Route.write_routes_json(routes))
    # f.close()



    # et = time.time()

    # elapsed_time = et - st
    # print('Execution time:', elapsed_time, 'seconds')

generate_data()