from functools import reduce
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as f
from pyspark.storagelevel import StorageLevel
from ml_ops.data_prep.workflow.processors.spark.processor import SparkProcessor


class RepartitionProcessor(
        SparkProcessor, type='repartition_processor', version='v1'):

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
                        'repartition': {
                            'type': 'object',
                            'properties': {
                                'num_partitions': {'type': 'number'},
                                'cols': {
                                    'type': 'array',
                                    'items': {'type': 'string'}
                                }
                            }
                        },
                        'coalesce': {
                            'type': 'object',
                            'properties': {
                                'num_partitions': {'type': 'number'},
                            }
                        },
                        'select_columns': {
                            'type': 'array',
                            'items': {
                                'type': 'string'
                            }
                        },
                        'filter_condition': {'type': 'string'},
                        'limit': {'type': 'number'}
                    },
                    'oneOf': [
                        {'required': ['repartition']},
                        {'required': ['coalesce']},
                    ]

                }
        }
    }

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        df = self.dependencies[0].df
        if 'repartition' in self.properties:
            repartition_config = self.properties['repartition']
            repartition_params = []
            if 'num_partitions' in repartition_config:
                num_partitions = repartition_config['num_partitions']
                repartition_params.append(num_partitions)
            if 'cols' in repartition_config:
                cols = repartition_config['cols']
                repartition_params.append(*cols)
            df = df.repartition(*repartition_params)
        elif 'coalesce' in self.properties:
            num_partitions = self.properties['coalesce']['num_partitions']
            df = df.coalesce(num_partitions)
        return df


class PersistProcessor(SparkProcessor, type='persist_processor', version='v1'):

    storage_level_map = {
        'DISK_ONLY': StorageLevel.DISK_ONLY,
        'DISK_ONLY_2': StorageLevel.DISK_ONLY_2,
        'MEMORY_ONLY': StorageLevel.MEMORY_ONLY,
        'MEMORY_ONLY_2': StorageLevel.MEMORY_ONLY_2,
        'MEMORY_AND_DISK': StorageLevel.MEMORY_AND_DISK,
        'MEMORY_AND_DISK_2': StorageLevel.MEMORY_AND_DISK_2,
        'OFF_HEAP': StorageLevel.OFF_HEAP,
    }

    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string'},
            'type': {
                'type': 'string'},
            'properties': {
                'type': 'object',
                'properties': {
                        'storage_level': {
                            'type': 'string',
                            'enum': [
                                'DISK_ONLY',
                                'DISK_ONLY_2',
                                'MEMORY_ONLY',
                                'MEMORY_ONLY_2',
                                'MEMORY_AND_DISK',
                                'MEMORY_AND_DISK_2',
                                'OFF_HEAP'],
                        },
                    'select_columns': {
                            'type': 'array',
                                    'items': {
                                        'type': 'string'}},
                    'filter_condition': {
                            'type': 'string'},
                    'limit': {
                            'type': 'number'}},
                'required': ['storage_level']}}}

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        storage_level_key = self.properties['storage_level']
        storage_level = self.storage_level_map[storage_level_key]
        df = self.dependencies[0].df.persist(storage_level)
        return df


class LoadProcessor(SparkProcessor, type='load_processor', version='v1'):

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
                        'path': {'type': 'string'},
                        'format': {'type': 'string'},
                        'options': {'type': 'object'},
                        'select_columns': {
                            'type': 'array',
                            'items': {
                                'type': 'string'
                            }
                        },
                        'filter_condition': {'type': 'string'},
                        'limit': {'type': 'number'}
                    },
                    'required': ['path', 'format']
                }
        }
    }

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        path = self.properties['path']
        format = self.properties['format']
        options = self.properties.get('options', {})
        df = self.spark.read.load(path=path, format=format, **options)
        return df


class ShowProcessor(SparkProcessor, type='show_processor', version='v1'):

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
                        'n': {'type': 'number'},
                        'truncate': {'type': 'boolean'},
                        'vertical': {'type': 'boolean'}
                    }
                }
        }
    }

    def run(self) -> DataFrame:
        n = self.properties.get('n', 20)
        truncate = self.properties.get('truncate', True)
        vertical = self.properties.get('vertical', False)
        df = self.dependencies[0].df
        df.show(n=n, truncate=truncate, vertical=vertical)
        return df


class JoinProcessor(SparkProcessor, type='join_processor', version='v1'):

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
                        'on': {
                            'type': 'array',
                            'items': {'type': 'string'}
                        },
                        'how': {'type': 'string'},
                        'select_columns': {
                            'type': 'array',
                            'items': {
                                'type': 'string'
                            }
                        },
                        'filter_condition': {'type': 'string'},
                        'limit': {'type': 'number'}
                    },
                    'required': ['on']
                }
        }
    }

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        on = self.properties['on']
        how = self.properties.get('how', 'inner')
        df = reduce(lambda x, y: x.join(y, on=on, how=how),
                    [dep.df for dep in self.dependencies])
        return df


class UnionProcessor(SparkProcessor, type='union_processor', version='v1'):

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        df = reduce(lambda x, y: x.union(y),
                    [dep.df for dep in self.dependencies])
        return df


class WriteProcessor(SparkProcessor, type='write_processor', version='v1'):

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
                        'path': {'type': 'string'},
                        'format': {'type': 'string'},
                        'mode': {'type': 'string'},
                        'partition_by': {
                            'type': 'array',
                            'items': {'type': 'string'}
                        },
                        'options': {'type': 'object'}
                    },
                    'required': ['path', 'format']
                }
        }
    }

    def run(self) -> DataFrame:
        path = self.properties['path']
        format = self.properties['format']
        mode = self.properties.get('mode', 'overwrite')
        partition_by = self.properties.get('partition_by', None)
        options = self.properties.get('options', {})
        source_df = self.dependencies[0].df
        source_df.write.save(path=path, format=format,
                             mode=mode, partitionBy=partition_by, **options)
        return source_df


class SQLProcessor(SparkProcessor, type='sql_processor', version='v1'):

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
                        'query': {'type': 'string'},
                        'select_columns': {
                            'type': 'array',
                            'items': {
                                'type': 'string'
                            }
                        },
                        'filter_condition': {'type': 'string'},
                        'limit': {'type': 'number'}
                    },
                    'required': ['query']
                }
        }
    }

    @property
    def mandatory_properties(self):
        return ['query']

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        for dep in self.dependencies:
            dep.df.createOrReplaceTempView(dep.alias)
        df = self.spark.sql(self.properties['query'])
        return df


class AggregateProcessor(
        SparkProcessor, type='aggregate_processor', version='v1'):

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
                        'group_by_columns': {
                            'type': 'array',
                            'items': {
                                'type': 'string'
                            }
                        },
                        'agg_exprs': {
                            'type': 'array',
                            'items': {
                                'type': 'string'
                            }
                        },
                        'select_columns': {
                            'type': 'array',
                            'items': {
                                'type': 'string'
                            }
                        },
                        'filter_condition': {'type': 'string'},
                        'limit': {'type': 'number'}
                    },
                    'required': ['group_by_columns', 'agg_exprs']
                }
        }
    }

    @property
    def mandatory_properties(self):
        return ['group_by_columns', 'agg_exprs']

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        group_by_columns = self.properties['group_by_columns']
        agg_exprs = [f.expr(x) for x in self.properties['agg_exprs']]
        df = self.dependencies[0].df.groupBy(group_by_columns).agg(*agg_exprs)
        return df
