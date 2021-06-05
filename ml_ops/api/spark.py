from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from flask import g


def get_spark(spark_config: dict):

    if 'spark' in g:
        return g.spark

    spark_conf = SparkConf().setAll(spark_config.items())
    g.spark = SparkSession.Builder.config(conf=spark_conf).getOrCreate()

    return g.spark


def get_spark_config():

    spark_config = {
        'spark.master': 'local[*]',
        'spark.app.name': 'ml_ops',
        'spark.sql.shuffle.partitions': '24'
    }

    if 'spark_config' in g:
        spark_config.update(g.spark_config)

    return spark_config
