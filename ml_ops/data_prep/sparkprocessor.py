from abc import ABC, abstractmethod
from functools import reduce
from typing import Dict, List
from networkx.classes.digraph import DiGraph
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as f
from pyspark.storagelevel import StorageLevel
from dataclasses import dataclass
from collections import Counter
import networkx as nx
import itertools
import jsonschema


@dataclass
class Dependency:
    df: DataFrame
    alias: str


class SparkProcessor(ABC):
    """
        Base class for post processor.
    """

    _types = {}

    @property
    def mandatory_properties(self):
        return []

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {'type': 'object'}
        }
    }

    @classmethod
    def validate(cls, config):
        # validate Spark post processor schema
        jsonschema.validate(instance=config, schema=cls.schema)

    @staticmethod
    def get_spark_processor(type: str):
        assert type in SparkProcessor._types, \
            f'SparkProcessor implementation not found for {type}'
        return SparkProcessor._types[type]

    def __init__(self, spark: SparkSession, name: str, properties: Dict = {}, depedencies: List[Dependency] = []) -> None:
        super().__init__()
        self.spark = spark
        self.name = name
        self.properties = properties
        missing_propeties = list(
            set(self.mandatory_properties).difference(properties.keys()))
        assert not bool(missing_propeties), \
            f'Missing mandatory properties {missing_propeties} for processor {self.name}!'
        self.dependencies = depedencies

    @classmethod
    def __init_subclass__(cls, type, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._types[type] = cls

    @abstractmethod
    def run(self) -> DataFrame:
        pass

    def select(fun):
        def select_inner(self):
            df = fun(self)
            if 'select_columns' in self.properties:
                df = df.selectExpr(self.properties['select_columns'])
            return df
        return select_inner

    def filter(fun):
        def filter_inner(self):
            df = fun(self)
            if 'filter_condition' in self.properties:
                df = df.filter(self.properties['filter_condition'])
            return df
        return filter_inner

    def limit(fun):
        def limit_inner(self):
            df = fun(self)
            if 'limit' in self.properties:
                df = df.limit(self.properties['limit'])
            return df
        return limit_inner


class RepartitionProcessor(SparkProcessor, type='repartition_processor'):

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
            repartition_params = []
            if 'num_partitions' in self.properties['repartition']:
                num_partitions = self.properties['repartition']['num_partitions']
                repartition_params.append(num_partitions)
            if 'cols' in self.properties['repartition']:
                cols = self.properties['repartition']['cols']
                repartition_params.append(*cols)
            df = df.repartition(*repartition_params)
        elif 'coalesce' in self.properties:
            num_partitions = self.properties['coalesce']['num_partitions']
            df = df.coalesce(num_partitions)
        return df


class PersistProcessor(SparkProcessor, type='persist_processor'):

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
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
                        'storage_level': {
                            'type': 'string',
                            'enum': ['DISK_ONLY', 'DISK_ONLY_2', 'MEMORY_ONLY', 'MEMORY_ONLY_2', 'MEMORY_AND_DISK', 'MEMORY_AND_DISK_2', 'OFF_HEAP'],
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
                    'required': ['storage_level']
                }
        }
    }

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        storage_level = self.storage_level_map[self.properties['storage_level']]
        df = self.dependencies[0].df.persist(storage_level)
        return df


class LoadProcessor(SparkProcessor, type='load_processor'):

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


class LoadStreamProcessor(SparkProcessor, type='load_stream_processor'):

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
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
                    'required': ['format']
                }
        }
    }

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        options = self.properties.get('options', {})

        load_params = {
            'path': self.properties.get('path', None),
            'format': self.properties.get['format'],
        }

        df = self.spark.readStream.load(**load_params, **options)
        return df


class WriteStreamProcessor(SparkProcessor, type='write_stream_processor'):

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
                        'output_mode': {
                            'type': 'string',
                                    'enum': ['append', 'complete', 'update']
                        },
                        'partition_by': {
                            'type': 'array',
                                    'items': {'type': 'string'}
                        },
                        'options': {'type': 'object'},
                        'trigger': {
                            'type': 'object',
                            'properties': {
                                'oneOf': [
                                    {
                                        'processing_time': {'type': 'string'}
                                    },
                                    {
                                        'once': {'type': 'boolean'}
                                    },
                                    {
                                        'continuous': {'type': 'string'}
                                    }
                                ]
                            }
                        }

                    },
                    'required': ['format', 'output_mode']
                }
        }
    }

    def run(self) -> DataFrame:
        options = self.properties.get('options', {})
        trigger_properties = self.get(
            'trigger', {'processing_time': '0 seconds'})
        trigger_params = {'processingTime': '0 seconds'}

        if 'processing_time' in trigger_properties.keys():
            trigger_params = {
                'processingTime': trigger_properties['processing_time']}
        elif 'once' in trigger_properties.keys():
            trigger_params = {'once': trigger_properties['once']}
        elif 'continuous' in trigger_properties.keys():
            trigger_params = {'continuous': trigger_properties['continuous']}

        start_params = {
            'path': self.properties.get('path', None),
            'format': self.properties.get['format'],
            'outputMode': self.properties.get['output_mode'],
            'partitionBy': self.properties.get('partition_by', None),
            'queryName': self.properties.get('query_name', None),
        }

        df = self.dependencies[0].df
        df.writeStream.trigger(**trigger_params).start(**
                                                       start_params, **options)
        return df


class ShowProcessor(SparkProcessor, type='show_processor'):

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


class JoinProcessor(SparkProcessor, type='join_processor'):

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


class UnionProcessor(SparkProcessor, type='union_processor'):

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        df = reduce(lambda x, y: x.union(y),
                    [dep.df for dep in self.dependencies])
        return df


class WriteProcessor(SparkProcessor, type='write_processor'):

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


class SQLProcessor(SparkProcessor, type='sql_processor'):

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


class AggregateProcessor(SparkProcessor, type='aggregate_processor'):

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


class SparkWorkflowManager:

    schema = {
        'type': 'object',
        'properties': {
                'processors': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'name': {'type': 'string'},
                            'type': {'type': 'string'},
                            'properties': {'type': 'object'}
                        }
                    }
                },
            'relations': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'left': {'type': 'string'},
                            'right': {'type': 'string'}
                        },
                        "required": ["left", "right"]
                    }
            }
        },
        'required': ['processors', 'relations']
    }

    @classmethod
    def validate(cls, config):
        jsonschema.validate(instance=config, schema=cls.schema)
        processor_configs = config['processors']
        for proc_config in processor_configs:
            processor: SparkProcessor = SparkProcessor.get_spark_processor(
                proc_config['type'])
            processor.validate(proc_config)

        # check for duplciate processor names
        processor_names = [processor['name']
                           for processor in config['processors']]
        duplicate_names = [item for item, count in Counter(
            processor_names).items() if count > 1]
        assert len(duplicate_names) == 0, \
            f'Processor names in {duplicate_names} is repeated. Processors should have unique names.'

    def __init__(self, config, spark) -> None:
        super().__init__()
        self.df_map = {}
        self.validate(config)
        self.config = config
        self.graph = nx.DiGraph()
        edges = [(relation['left'], relation['right'])
                 for relation in self.config['relations']]
        self.graph.add_edges_from(edges)
        self.spark = spark

    def process_composition(self, node, config, graph: DiGraph, session) -> SparkProcessor:
        properties = config[node].get('properties', {})
        processor_name = config[node]['name']
        processor_type = config[node]['type']
        predecessors = list(graph.predecessors(node))
        dependencies = []
        if not bool(predecessors):
            predecessors = []
        for predecessor in predecessors:
            prev_processor = self.process_composition(
                predecessor, config, graph, session)
            try:
                df = self.df_map.get(predecessor, prev_processor.run())
            except Exception as error:
                raise error
            dependencies.append(Dependency(df, predecessor))
        processor = SparkProcessor.get_spark_processor(
            processor_type)(session, processor_name, properties, dependencies)
        return processor

    def run(self):

        assert 'processors' in self.config, 'Missing "processors" in {self.config}!'

        processor_mandatory_keys = ['name', 'type']
        for processor, key in itertools.product(self.config['processors'], processor_mandatory_keys):
            assert key in processor, f'Missing mandatory processor config "{key}" in {processor}!'

        sink_nodes = [n for n, d in self.graph.out_degree() if d == 0]
        processor_config_map = {
            processor['name']: processor for processor in self.config['processors']}

        output_dfs = []
        for node in sink_nodes:
            processor = self.process_composition(node, processor_config_map, graph=self.graph,
                                                 session=self.spark)
            output_dfs.append(processor.run())
        return output_dfs
