import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    conf = SparkConf()
    conf.set("spark.master", "local[1]")
    conf.set("spark.app.name", "tests_helper")
    conf.set("spark.sql.shuffle.partitions", "3")
    return SparkSession.builder.config(conf=conf).getOrCreate()
