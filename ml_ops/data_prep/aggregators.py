from functools import singledispatch
from typing import List
from ml_ops.session.session import Dependency

from pyspark.sql import SparkSession, DataFrame


@singledispatch
def aggregate(session, *args, **kwargs):
    pass

@aggregate.register
def _(session: SparkSession, dependencies: List[Dependency], *args, **kwargs):

    group_by_cols_key = 'group_by_cols'
    agg_exprs_key = 'agg_exprs'

    df = dependencies[0]
    return df.groupBy(kwargs[group_by_cols_key]).agg(kwargs[agg_exprs_key])
