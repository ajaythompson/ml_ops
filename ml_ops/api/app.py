from flask import Flask
from flask_cors import CORS
from pyspark.conf import SparkConf

app = Flask(__name__)
CORS(app)
# app.config.from_envvar('MLOPS_CONFIG_PATH')
app.config

# Set the secret key to some random bytes. Keep this really secret!
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'


def init_spark(spark_config: dict = None):
    if spark_config is None:
        spark_config = {
            'spark.master': 'local[*]',
            'spark.app.name': 'ml_ops',
            'spark.sql.shuffle.partitions': '24'
        }

    spark_conf = SparkConf().setAll(spark_config.items())

    app.app_context.spark_session = 


if 'spark' not in g:
    conf = SparkConf()
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "tests_helper")
    conf.set("spark.sql.shuffle.partitions", "8")
    g.sc = SparkSession.builder.config(conf=conf).getOrCreate()
    return g.sc
