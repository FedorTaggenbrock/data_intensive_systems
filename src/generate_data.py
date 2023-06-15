"""

File which generates the dataset and save it in the folder called "data"

"""

import random
import json
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
        
        def permute(self, prob, verbose = False):
            """ This function permuts the amount of a product given some percentage """
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

            if r >= 0 and r < 1/3:
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

            if r >= 1/3 and r <  2/3:
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
            r = random.random() * 3

            # TODO: change probility intervals

            if r >= probability * 0 and r < probability * 1:
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
                
            if r >= probability * 1 and r < probability * 2:
                """ remove a random stop in the list """
                stop = random.choice(self.stops)
                idx  = self.stops.index(stop)

                # only performe linked-list remove operation if not start or end
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
        def write_routes_json(routes: list) -> str:
            s = "["
            for r in routes:
                s += (str(r) + ",")
            s = s[:-1] + ']'
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


    # routes = Route.generate_routes(
    #     10,
    #     city_count=range(5, 10), 
    #     product_range=range(2, 5), 
    #     product_count=range(5, 20),
    #     city_list=cities, 
    #     product_list=products, 
    # )


    routes = Route.read_routes_json(
        '10_standard_route.json', 
        mutate_product_range=range(2, 5), 
        mutate_product_count=range(5, 20), 
        product_list=products, 
        city_list=cities
    )

    permuted = []
    n = 100
    permutation = 0.25

    def permutate_route(id, route, p):
        c = 0
        id_ = 0

        for i in range(n):
            route_ = copy.deepcopy(route)
            
            route_.permute(p)
            route_.sr = route.id
            route_.id = id_
            permuted.append(route_)
            id_ += 1

            if (100/n*i) % 10 == 1:
                c+=1
                print(f"Thread {id}: {c*10}%")

    st = time.time()

    threads = []
    for route in routes:
        print(f"Creating thread {route.id}")
        thread = Thread(target= permutate_route, args=[route.id, route, permutation])
        threads.append(thread)
        thread.start()

    while threading.active_count() > 1:
        for t in threads:
            t.join()
            print (f"{t}, is done.")
    print("Done permutating")

    f = open(f"{n*len(routes)}_{permutation}_actual_routes.json", "w")
    f.write(Route.write_routes_json(permuted))
    f.close()
        

    f = open(f"{len(routes)}_standard_route.json", "w")
    f.write(Route.write_routes_json(routes))
    f.close()



    et = time.time()

    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')

    