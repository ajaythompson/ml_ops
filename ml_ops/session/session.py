from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import collections


def get_spark_session(spark_options_pairs):
    spark_conf = SparkConf()
    spark_conf.setAll(spark_options_pairs)
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()


Dependency = collections.namedtuple('Dependency', ['df', 'alias'])