from functools import singledispatch, partial
from typing import List

from networkx.algorithms.shortest_paths.unweighted import predecessor
from ml_ops.session.session import Dependency
from pyspark import conf
from pyspark.conf import SparkConf
from ml_ops.data_prep import data_loader, data_writer
from pyspark.sql import SparkSession
import networkx as nx
from abc import ABC, abstractmethod


@singledispatch
def compose(processor, config):
    # config = SparkConf()
    # config.set('spark.app.name', 'wrangle')
    # config.set('spark.master', 'local[1]')
    # spark_session = SparkSession.builder.config(conf=config).getOrCreate()

    # read_options = {'header': 'true', 'inferSchema': 'true'}
    # load_data = data_loader.load_data(read_options=read_options, )
    pass


@compose.register
def _(session: SparkSession, config):
    read_options = {'header': 'true', 'inferSchema': 'true'}
    load_data = data_loader.load_data(read_options=read_options, )


processor_type_map = {
    'load': data_loader.load_data,
    'write': data_writer.write,
}


def get_processor(type, processor_config):
    return partial(processor_type_map[type], **processor_config)


G = nx.DiGraph()
G.add_nodes_from(['load', 'write'])
G.add_edges_from([('load', 'write')])

config = {
    'load': {
        'type': 'load',
        'config': {
            'path': './load.csv',
            'format': 'csv',
            'options': {
                'header': 'true'
            }
        }
    },
    'write': {
        'type': 'write',
        'config': {
            'path': './write.csv',
            'format': 'csv',
            'options': {
                'header': 'true'
            }
        }
    },
}

source_nodes = [n for n, d in G.in_degree() if d == 0]
sink_nodes = [n for n, d in G.out_degree() if d == 0]
print(sink_nodes)

spark_conf = SparkConf()
spark_conf.set('spark.app.name', 'wrangle')
spark_conf.set('spark.master', 'local[1]')
spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# read_options = {'header': 'true', 'inferSchema': 'true'}
# load_data = data_loader.load_data(read_options=read_options, )


def process_composition(node, config, graph, session):
    node_config = config[node]['config']
    processor_type = config[node]['type']
    predecessors = G.predecessors(node)
    processor_fun = get_processor(processor_type, node_config)
    dependency_list = []
    if bool(predecessors):
        dependency_list = [Dependency(process_composition(predecessor, config, graph, session), predecessor)
                           for predecessor in predecessors]
    return processor_fun(session, dependency_list)


for node in sink_nodes:
    process_composition(node=node, config=config,
                        graph=G, session=spark_session)


class DataPrepComposer(ABC):
    def __init__(self, edges, config) -> None:
        super().__init__()
        self.edges = edges
        self.config = config

    @abstractmethod
    def compose(self):
        pass

def process_composition(node, config, graph, session):
    node_config = config[node]['config']
    processor_type = config[node]['type']
    predecessors = G.predecessors(node)
    processor_fun = get_processor(processor_type, node_config)
    dependency_list = []
    if bool(predecessors):
        dependency_list = [Dependency(process_composition(predecessor, config, graph, session), predecessor)
                           for predecessor in predecessors]
    return processor_fun(session, dependency_list)

class SparkDataPrepComposer(DataPrepComposer):

    def __init__(self, edges, config, spark_options) -> None:
        super().__init__(edges, config)
        spark_conf = SparkConf(list(spark_options))
        self.spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        self.cache_map = {}


    def compose(self):
        G = nx.DiGraph()
        G.G.add_edges_from(self.edges)
        sink_nodes = [n for n, d in G.out_degree() if d == 0]
        for node in sink_nodes:
            process_composition(node, self.config, graph=G, session=spark_session)