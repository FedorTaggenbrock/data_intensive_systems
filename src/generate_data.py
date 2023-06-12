"""

File which generates the dataset and save it in the folder called "data"

"""

import random
import json
import copy
from threading import Thread
import threading
import time

# 250 cities
cities = ["city_0", "city_1", "city_2", "city_3", "city_4", "city_5", "city_6", "city_7", "city_8", "city_9", "city_10", "city_11", "city_12", "city_13", "city_14", "city_15", "city_16", "city_17", "city_18", "city_19", "city_20", "city_21", "city_22", "city_23", "city_24", "city_25", "city_26", "city_27", "city_28", "city_29", "city_30", "city_31", "city_32", "city_33", "city_34", "city_35", "city_36", "city_37", "city_38", "city_39", "city_40", "city_41", "city_42", "city_43", "city_44", "city_45", "city_46", "city_47", "city_48", "city_49", "city_50", "city_51", "city_52", "city_53", "city_54", "city_55", "city_56", "city_57", "city_58", "city_59", "city_60", "city_61", "city_62", "city_63", "city_64", "city_65", "city_66", "city_67", "city_68", "city_69", "city_70", "city_71", "city_72", "city_73", "city_74", "city_75", "city_76", "city_77", "city_78", "city_79", "city_80", "city_81", "city_82", "city_83", "city_84", "city_85", "city_86", "city_87", "city_88", "city_89", "city_90", "city_91", "city_92", "city_93", "city_94", "city_95", "city_96", "city_97", "city_98", "city_99", "city_100", "city_101", "city_102", "city_103", "city_104", "city_105", "city_106", "city_107", "city_108", "city_109", "city_110", "city_111", "city_112", "city_113", "city_114", "city_115", "city_116", "city_117", "city_118", "city_119", "city_120", "city_121", "city_122", "city_123", "city_124", "city_125", "city_126", "city_127", "city_128", "city_129", "city_130", "city_131", "city_132", "city_133", "city_134", "city_135", "city_136", "city_137", "city_138", "city_139", "city_140", "city_141", "city_142", "city_143", "city_144", "city_145", "city_146", "city_147", "city_148", "city_149", "city_150", "city_151", "city_152", "city_153", "city_154", "city_155", "city_156", "city_157", "city_158", "city_159", "city_160", "city_161", "city_162", "city_163", "city_164", "city_165", "city_166", "city_167", "city_168", "city_169", "city_170", "city_171", "city_172", "city_173", "city_174", "city_175", "city_176", "city_177", "city_178", "city_179", "city_180", "city_181", "city_182", "city_183", "city_184", "city_185", "city_186", "city_187", "city_188", "city_189", "city_190", "city_191", "city_192", "city_193", "city_194", "city_195", "city_196", "city_197", "city_198", "city_199", "city_200", "city_201", "city_202", "city_203", "city_204", "city_205", "city_206", "city_207", "city_208", "city_209", "city_210", "city_211", "city_212", "city_213", "city_214", "city_215", "city_216", "city_217", "city_218", "city_219", "city_220", "city_221", "city_222", "city_223", "city_224", "city_225", "city_226", "city_227", "city_228", "city_229", "city_230", "city_231", "city_232", "city_233", "city_234", "city_235", "city_236", "city_237", "city_238", "city_239", "city_240", "city_241", "city_242", "city_243", "city_244", "city_245", "city_246", "city_247", "city_248", "city_249"]

# 1000 products
products = ["product_0", "product_1", "product_2", "product_3", "product_4", "product_5", "product_6", "product_7", "product_8", "product_9", "product_10", "product_11", "product_12", "product_13", "product_14", "product_15", "product_16", "product_17", "product_18", "product_19", "product_20", "product_21", "product_22", "product_23", "product_24", "product_25", "product_26", "product_27", "product_28", "product_29", "product_30", "product_31", "product_32", "product_33", "product_34", "product_35", "product_36", "product_37", "product_38", "product_39", "product_40", "product_41", "product_42", "product_43", "product_44", "product_45", "product_46", "product_47", "product_48", "product_49", "product_50", "product_51", "product_52", "product_53", "product_54", "product_55", "product_56", "product_57", "product_58", "product_59", "product_60", "product_61", "product_62", "product_63", "product_64", "product_65", "product_66", "product_67", "product_68", "product_69", "product_70", "product_71", "product_72", "product_73", "product_74", "product_75", "product_76", "product_77", "product_78", "product_79", "product_80", "product_81", "product_82", "product_83", "product_84", "product_85", "product_86", "product_87", "product_88", "product_89", "product_90", "product_91", "product_92", "product_93", "product_94", "product_95", "product_96", "product_97", "product_98", "product_99", "product_100", "product_101", "product_102", "product_103", "product_104", "product_105", "product_106", "product_107", "product_108", "product_109", "product_110", "product_111", "product_112", "product_113", "product_114", "product_115", "product_116", "product_117", "product_118", "product_119", "product_120", "product_121", "product_122", "product_123", "product_124", "product_125", "product_126", "product_127", "product_128", "product_129", "product_130", "product_131", "product_132", "product_133", "product_134", "product_135", "product_136", "product_137", "product_138", "product_139", "product_140", "product_141", "product_142", "product_143", "product_144", "product_145", "product_146", "product_147", "product_148", "product_149", "product_150", "product_151", "product_152", "product_153", "product_154", "product_155", "product_156", "product_157", "product_158", "product_159", "product_160", "product_161", "product_162", "product_163", "product_164", "product_165", "product_166", "product_167", "product_168", "product_169", "product_170", "product_171", "product_172", "product_173", "product_174", "product_175", "product_176", "product_177", "product_178", "product_179", "product_180", "product_181", "product_182", "product_183", "product_184", "product_185", "product_186", "product_187", "product_188", "product_189", "product_190", "product_191", "product_192", "product_193", "product_194", "product_195", "product_196", "product_197", "product_198", "product_199", "product_200", "product_201", "product_202", "product_203", "product_204", "product_205", "product_206", "product_207", "product_208", "product_209", "product_210", "product_211", "product_212", "product_213", "product_214", "product_215", "product_216", "product_217", "product_218", "product_219", "product_220", "product_221", "product_222", "product_223", "product_224", "product_225", "product_226", "product_227", "product_228", "product_229", "product_230", "product_231", "product_232", "product_233", "product_234", "product_235", "product_236", "product_237", "product_238", "product_239", "product_240", "product_241", "product_242", "product_243", "product_244", "product_245", "product_246", "product_247", "product_248", "product_249", "product_250", "product_251", "product_252", "product_253", "product_254", "product_255", "product_256", "product_257", "product_258", "product_259", "product_260", "product_261", "product_262", "product_263", "product_264", "product_265", "product_266", "product_267", "product_268", "product_269", "product_270", "product_271", "product_272", "product_273", "product_274", "product_275", "product_276", "product_277", "product_278", "product_279", "product_280", "product_281", "product_282", "product_283", "product_284", "product_285", "product_286", "product_287", "product_288", "product_289", "product_290", "product_291", "product_292", "product_293", "product_294", "product_295", "product_296", "product_297", "product_298", "product_299", "product_300", "product_301", "product_302", "product_303", "product_304", "product_305", "product_306", "product_307", "product_308", "product_309", "product_310", "product_311", "product_312", "product_313", "product_314", "product_315", "product_316", "product_317", "product_318", "product_319", "product_320", "product_321", "product_322", "product_323", "product_324", "product_325", "product_326", "product_327", "product_328", "product_329", "product_330", "product_331", "product_332", "product_333", "product_334", "product_335", "product_336", "product_337", "product_338", "product_339", "product_340", "product_341", "product_342", "product_343", "product_344", "product_345", "product_346", "product_347", "product_348", "product_349", "product_350", "product_351", "product_352", "product_353", "product_354", "product_355", "product_356", "product_357", "product_358", "product_359", "product_360", "product_361", "product_362", "product_363", "product_364", "product_365", "product_366", "product_367", "product_368", "product_369", "product_370", "product_371", "product_372", "product_373", "product_374", "product_375", "product_376", "product_377", "product_378", "product_379", "product_380", "product_381", "product_382", "product_383", "product_384", "product_385", "product_386", "product_387", "product_388", "product_389", "product_390", "product_391", "product_392", "product_393", "product_394", "product_395", "product_396", "product_397", "product_398", "product_399", "product_400", "product_401", "product_402", "product_403", "product_404", "product_405", "product_406", "product_407", "product_408", "product_409", "product_410", "product_411", "product_412", "product_413", "product_414", "product_415", "product_416", "product_417", "product_418", "product_419", "product_420", "product_421", "product_422", "product_423", "product_424", "product_425", "product_426", "product_427", "product_428", "product_429", "product_430", "product_431", "product_432", "product_433", "product_434", "product_435", "product_436", "product_437", "product_438", "product_439", "product_440", "product_441", "product_442", "product_443", "product_444", "product_445", "product_446", "product_447", "product_448", "product_449", "product_450", "product_451", "product_452", "product_453", "product_454", "product_455", "product_456", "product_457", "product_458", "product_459", "product_460", "product_461", "product_462", "product_463", "product_464", "product_465", "product_466", "product_467", "product_468", "product_469", "product_470", "product_471", "product_472", "product_473", "product_474", "product_475", "product_476", "product_477", "product_478", "product_479", "product_480", "product_481", "product_482", "product_483", "product_484", "product_485", "product_486", "product_487", "product_488", "product_489", "product_490", "product_491", "product_492", "product_493", "product_494", "product_495", "product_496", "product_497", "product_498", "product_499", "product_500", "product_501", "product_502", "product_503", "product_504", "product_505", "product_506", "product_507", "product_508", "product_509", "product_510", "product_511", "product_512", "product_513", "product_514", "product_515", "product_516", "product_517", "product_518", "product_519", "product_520", "product_521", "product_522", "product_523", "product_524", "product_525", "product_526", "product_527", "product_528", "product_529", "product_530", "product_531", "product_532", "product_533", "product_534", "product_535", "product_536", "product_537", "product_538", "product_539", "product_540", "product_541", "product_542", "product_543", "product_544", "product_545", "product_546", "product_547", "product_548", "product_549", "product_550", "product_551", "product_552", "product_553", "product_554", "product_555", "product_556", "product_557", "product_558", "product_559", "product_560", "product_561", "product_562", "product_563", "product_564", "product_565", "product_566", "product_567", "product_568", "product_569", "product_570", "product_571", "product_572", "product_573", "product_574", "product_575", "product_576", "product_577", "product_578", "product_579", "product_580", "product_581", "product_582", "product_583", "product_584", "product_585", "product_586", "product_587", "product_588", "product_589", "product_590", "product_591", "product_592", "product_593", "product_594", "product_595", "product_596", "product_597", "product_598", "product_599", "product_600", "product_601", "product_602", "product_603", "product_604", "product_605", "product_606", "product_607", "product_608", "product_609", "product_610", "product_611", "product_612", "product_613", "product_614", "product_615", "product_616", "product_617", "product_618", "product_619", "product_620", "product_621", "product_622", "product_623", "product_624", "product_625", "product_626", "product_627", "product_628", "product_629", "product_630", "product_631", "product_632", "product_633", "product_634", "product_635", "product_636", "product_637", "product_638", "product_639", "product_640", "product_641", "product_642", "product_643", "product_644", "product_645", "product_646", "product_647", "product_648", "product_649", "product_650", "product_651", "product_652", "product_653", "product_654", "product_655", "product_656", "product_657", "product_658", "product_659", "product_660", "product_661", "product_662", "product_663", "product_664", "product_665", "product_666", "product_667", "product_668", "product_669", "product_670", "product_671", "product_672", "product_673", "product_674", "product_675", "product_676", "product_677", "product_678", "product_679", "product_680", "product_681", "product_682", "product_683", "product_684", "product_685", "product_686", "product_687", "product_688", "product_689", "product_690", "product_691", "product_692", "product_693", "product_694", "product_695", "product_696", "product_697", "product_698", "product_699", "product_700", "product_701", "product_702", "product_703", "product_704", "product_705", "product_706", "product_707", "product_708", "product_709", "product_710", "product_711", "product_712", "product_713", "product_714", "product_715", "product_716", "product_717", "product_718", "product_719", "product_720", "product_721", "product_722", "product_723", "product_724", "product_725", "product_726", "product_727", "product_728", "product_729", "product_730", "product_731", "product_732", "product_733", "product_734", "product_735", "product_736", "product_737", "product_738", "product_739", "product_740", "product_741", "product_742", "product_743", "product_744", "product_745", "product_746", "product_747", "product_748", "product_749", "product_750", "product_751", "product_752", "product_753", "product_754", "product_755", "product_756", "product_757", "product_758", "product_759", "product_760", "product_761", "product_762", "product_763", "product_764", "product_765", "product_766", "product_767", "product_768", "product_769", "product_770", "product_771", "product_772", "product_773", "product_774", "product_775", "product_776", "product_777", "product_778", "product_779", "product_780", "product_781", "product_782", "product_783", "product_784", "product_785", "product_786", "product_787", "product_788", "product_789", "product_790", "product_791", "product_792", "product_793", "product_794", "product_795", "product_796", "product_797", "product_798", "product_799", "product_800", "product_801", "product_802", "product_803", "product_804", "product_805", "product_806", "product_807", "product_808", "product_809", "product_810", "product_811", "product_812", "product_813", "product_814", "product_815", "product_816", "product_817", "product_818", "product_819", "product_820", "product_821", "product_822", "product_823", "product_824", "product_825", "product_826", "product_827", "product_828", "product_829", "product_830", "product_831", "product_832", "product_833", "product_834", "product_835", "product_836", "product_837", "product_838", "product_839", "product_840", "product_841", "product_842", "product_843", "product_844", "product_845", "product_846", "product_847", "product_848", "product_849", "product_850", "product_851", "product_852", "product_853", "product_854", "product_855", "product_856", "product_857", "product_858", "product_859", "product_860", "product_861", "product_862", "product_863", "product_864", "product_865", "product_866", "product_867", "product_868", "product_869", "product_870", "product_871", "product_872", "product_873", "product_874", "product_875", "product_876", "product_877", "product_878", "product_879", "product_880", "product_881", "product_882", "product_883", "product_884", "product_885", "product_886", "product_887", "product_888", "product_889", "product_890", "product_891", "product_892", "product_893", "product_894", "product_895", "product_896", "product_897", "product_898", "product_899", "product_900", "product_901", "product_902", "product_903", "product_904", "product_905", "product_906", "product_907", "product_908", "product_909", "product_910", "product_911", "product_912", "product_913", "product_914", "product_915", "product_916", "product_917", "product_918", "product_919", "product_920", "product_921", "product_922", "product_923", "product_924", "product_925", "product_926", "product_927", "product_928", "product_929", "product_930", "product_931", "product_932", "product_933", "product_934", "product_935", "product_936", "product_937", "product_938", "product_939", "product_940", "product_941", "product_942", "product_943", "product_944", "product_945", "product_946", "product_947", "product_948", "product_949", "product_950", "product_951", "product_952", "product_953", "product_954", "product_955", "product_956", "product_957", "product_958", "product_959", "product_960", "product_961", "product_962", "product_963", "product_964", "product_965", "product_966", "product_967", "product_968", "product_969", "product_970", "product_971", "product_972", "product_973", "product_974", "product_975", "product_976", "product_977", "product_978", "product_979", "product_980", "product_981", "product_982", "product_983", "product_984", "product_985", "product_986", "product_987", "product_988", "product_989", "product_990", "product_991", "product_992", "product_993", "product_994", "product_995", "product_996", "product_997", "product_998", "product_999"]

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