import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.sql.functions import udf, sum
from urllib.parse import urlparse
from urllib.parse import parse_qs
from datetime import datetime
from enum import Enum


class Product:
    def __init__(self, product_string):
        self.number_of_items = 0
        self.total_revenue = 0.0
        try:
            product = product_string.split(";")
            self.category = str(product[0])
            self.product_name = str(product[1])
            self.number_of_items = int(product[2]) if product[2].isnumeric() else 0
            self.total_revenue = float(product[3]) if product[3].isnumeric() else 0.0
            self.custom_event = str(product[4])
            self.merchandising_evar = str(product[5])
        except IndexError:
            pass
        except Exception:
            raise


class Events(Enum):
    Purchase = 1
    Product_View = 2
    Shopping_Cart_Open = 10
    Shopping_Cart_Checkout = 11
    Shopping_Cart_Add = 12
    Shopping_Cart_Remove = 13
    Shopping_Cart_View = 14


external_search_engines = ["google.com",
                           "yahoo.com",
                           "bing.com"]


def get_search_engine_domain_from_referrer(referrer: str) -> str:
    parsed = urlparse(referrer)
    return str(parsed.netloc)


def get_search_keyword_from_referrer(referrer: str) -> str:
    parsed = urlparse(referrer)
    elements = parse_qs(parsed.query)
    if 'q' in elements:
        return ' '.join(elements['q']).lower()
    elif 'p' in elements:
        return ' '.join(elements['p']).lower()
    else:
        return ""


def has_purchase_event(event_list: str) -> bool:
    if Events.Purchase.value in [int(evt) if evt.strip() else -999 for evt in str(event_list).split(",")]:
        return True
    else:
        return False


def calculate_revenue_from_product_list(product_list: str) -> float:
    products = [Product(product) for product in str(product_list).split(",")]
    total_revenue = 0
    for product in products:
        total_revenue += product.total_revenue * product.number_of_items
    return total_revenue


def get_output_file_nm(base_folder: str, file_name: str) -> str:
    curr_dt = datetime.now().strftime('%Y-%m-%d')
    return base_folder + "/" + curr_dt + "_" + file_name


def is_external_search_engine(search_engine_domain: str) -> bool:
    for se in external_search_engines:
        if se in search_engine_domain:
            return True
    return False


udf_get_search_engine_domain_from_referrer = udf(lambda m: get_search_engine_domain_from_referrer(m))
udf_get_search_keyword_from_referrer = udf(lambda m: get_search_keyword_from_referrer(m))
udf_has_purchase_event = udf(lambda m: has_purchase_event(m), BooleanType())
udf_calculate_revenue_from_product_list = udf(lambda m: calculate_revenue_from_product_list(m), FloatType())
udf_is_external_search_engine = udf(lambda m: is_external_search_engine(m), BooleanType())


class ProcessHitLevelSpark:
    schema = StructType([StructField('hit_time_gmt', IntegerType(), True),
                         StructField('date_time', StringType(), True),
                         StructField('user_agent', StringType(), True),
                         StructField('ip', StringType(), True),
                         StructField('event_list', StringType(), True),
                         StructField('geo_city', StringType(), True),
                         StructField('geo_region', StringType(), True),
                         StructField('geo_country', StringType(), True),
                         StructField('pagename', StringType(), True),
                         StructField('page_url', StringType(), True),
                         StructField('product_list', StringType(), True),
                         StructField('referrer', StringType(), True)])

    df = None
    processed = None
    conf = SparkConf().setMaster("local").setAppName("HitLevelETLSpark")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    def __int__(self):
        pass

    def input(self, input_path, separator):
        self.df = self.spark.read.csv(path=input_path,
                                      schema=self.schema,
                                      header=True,
                                      sep=separator).na.fill(value="")

    def process(self):
        filtered_ext = self.df.filter(udf_is_external_search_engine(self.df.referrer))
        filtered_pur = self.df.filter(udf_has_purchase_event(self.df.event_list.cast("string")))
        joined = filtered_pur.alias("t1").join(filtered_ext.alias("t2"), "ip", 'inner')
        projected = joined.select(udf_get_search_engine_domain_from_referrer(joined['t2.referrer'])
                                  .alias("search_engine_domain"),
                                  udf_get_search_keyword_from_referrer(joined['t2.referrer'])
                                  .alias("search_keyword"),
                                  udf_calculate_revenue_from_product_list(joined['t1.product_list'])
                                  .alias("revenue"))
        self.processed = projected.groupby(['search_engine_domain', 'search_keyword']) \
            .agg(sum('revenue').alias("revenue"))

    def output(self, base_folder):
        file_name = "SearchKeywordPerformance.tab"
        target = get_output_file_nm(base_folder, file_name)
        self.processed.write.options(header=True, delimiter='\t').csv(target)


if __name__ == "__main__":
    in_file = sys.argv[1]
    base_output = sys.argv[2]
    etl = ProcessHitLevelSpark()
    etl.input(in_file, '\t')
    etl.process()
    etl.output(base_output)
