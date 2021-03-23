from functools import singledispatch

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


@singledispatch
def load_data(session, dependencies, *args, **kwargs):
    pass

@load_data.register
def _(session: SparkSession, dependencies, *args, **kwargs) -> DataFrame:
    read_options_key = 'options'
    path_key = 'path'
    format_key = 'format'

    return session.read.load(path=kwargs[path_key], format=kwargs[format_key], **kwargs[read_options_key])
