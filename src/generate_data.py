"""

File which generates the dataset and save it in the folder called "data"

"""

import random

# 30 cities
cities = ["Amsterdam", "Rotterdam", "Den Haag", "Utrecht", "Eindhoven", "Tilburg", "Groningen", "Almere", "Breda", "Nijmegen", "Enschede", "Haarlem", "Arnhem", "Zaanstad", "Amersfoort", "Apeldoorn", "Hoofddorp", "Maastricht", "Leiden", "Dordrecht", "Zwolle", "Emmen", "Alkmaar", "Lelystad", "Leeuwarden", "Venlo", "Delft", "Deventer", "Sittard", "Helmond"]
# 50 products 
products = ["cheese", "milk", "bread", "eggs", "yogurt", "chicken", "beef", "pork", "apples", "bananas", "oranges", "strawberries", "tomatoes", "cucumbers", "lettuce", "carrots", "potatoes", "onions", "rice", "pasta", "cereal", "oatmeal", "butter", "jam", "cookies", "chips", "soda", "coffee", "tea", "juice", "water", "chocolate", "icecream", "pizza", "honey", "mayo", "ketchup", "mustard", "pickles", "yogurt", "peanutbutter", "popcorn", "salt", "sugar", "flour", "oil", "vinegar", "sauce", "soup", "soap", "tissues"]

class Product:
    def __init__(self, name, count):
        self.name = name
        self.count = count

    def __str__(self) -> str:
        return '"' + str(self.name) + '":' + str(self.count)

class Stop:
    def __init__(self, origin, destination, products = set()):
        self.origin = origin
        self.destination = destination
        self.products = products

    def generateProducts(self, products:range, count:range, product_list:list, random_):
        self.products = set()
        p = int(random_(products.start, products.stop))

        # TODO: only add products that are not already in the list
        while len(self.products) <= p:
            c = int(random_(count.start, count.stop))
            i = random.choice(product_list)

            self.products.add( Product(i, c) )

    def __str__(self) -> str:
        s =  '{"from":"' + self.origin + '","to":"' + self.destination + '","merchandise":{'
        for p in self.products:
            s += str(p) + ","
        return s[:-1] + "}},"

class Route:
    stops = []

    def __init__(self, id):
        self.id = id
        self.stops = []

    def __str__(self) -> str:
        s = '{"id":' + str(self.id) + ', "route":['
        for x in self.stops:
            s += str(x)
        return s[:-1] + ']}'

    @staticmethod
    def generateRoutes(x, city_count:range, products:range, product_count:range, city_list:list, product_list: list, random_):
        routes = []

        for i in range(x): 
            r = Route(i)
            r.generateStops(city_count, products, product_count, city_list, product_list, random_)
            routes.append( r )

        return routes

    # @staticmethod
    # def tojson(routes):
    #     s = "["
    #     for r in routes:
    #         s += (str(r) + ",")
    #     return s[:-1] + ']'


    def generateStops(self, city_count:range, products:range, product_count:range, city_list:list, product_list: list, random_):
        s = int(random_(city_count.start, city_count.stop))

        while (len(self.stops) <= s):

            if (len(self.stops) == 0):  stop_1 = random.choice(city_list)
            else:                       stop_1 = self.stops[len(self.stops) - 1].destination
            stop_2 = random.choice(city_list)

            if (stop_1 == stop_2):continue
            

            stop = Stop(stop_1, stop_2)
            stop.generateProducts(products, product_count, product_list, random_)

            self.stops.append( stop )
        
    def permuteRoute(self, permutations, std):
        pass

              
x = Route.generateRoutes(
    5,
    city_count=range(5, 10), 
    products=range(2, 5), 
    product_count=range(5, 20),
    city_list=cities, 
    product_list=products, 
    random_=random.uniform
)

s = "["
for r in x:
    s += (str(r) + ",")
s = s[:-1] + ']'

print(s)
