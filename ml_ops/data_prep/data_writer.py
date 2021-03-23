from functools import singledispatch
from typing import List
from ml_ops.session.session import Dependency

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


@singledispatch
def write(session, *args, **kwargs):
    pass

@write.register
def _(session: SparkSession, dependencies: List[Dependency], *args, **kwargs):
    write_options_key = 'options'
    write_format_key = 'format'
    write_path_key = 'path'


    write_df: DataFrame = dependencies[0].df
    write_options = kwargs[write_options_key]
    write_format = kwargs[write_format_key]
    write_path = kwargs[write_path_key]

    write_df.write.save(format=write_format, path=write_path, **write_options)