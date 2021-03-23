from functools import singledispatch

from pyspark.sql import SparkSession


@singledispatch
def generate(session, *args, **kwargs):
    pass

@generate.register
def _(session: SparkSession, *args, **kwargs):
    source_df_alias_key = 'source_df_alias_list'
    feature_query_key = 'feature_query'

    source_df_alias_list = kwargs[source_df_alias_key]
    feature_query = kwargs[feature_query_key]

    for df, alias in source_df_alias_list:
        df.createOrReplaceTempView(alias)

    return session.sql(feature_query)