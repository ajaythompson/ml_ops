from abc import ABC, abstractmethod
from functools import partial

from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from ml_ops.session.session import Dependency
from ml_ops.data_prep import data_loader, data_writer
import networkx as nx


class DataPrepComposer(ABC):
    def __init__(self, edges, config) -> None:
        super().__init__()
        self.config = config
        self.graph = nx.DiGraph()
        self.graph.add_edges_from(self.edges)

    @abstractmethod
    def __call__(self):
        pass


processor_type_map = {
    'load': data_loader.load_data,
    'write': data_writer.write,
}


def get_processor(type, processor_config):
    return partial(processor_type_map[type], **processor_config)


def process_composition(node, config, graph, session):
    node_config = config[node]['config']
    processor_type = config[node]['type']
    predecessors = graph.predecessors(node)
    processor_fun = get_processor(processor_type, node_config)
    dependency_list = []
    if bool(predecessors):
        dependency_list = [Dependency(process_composition(predecessor, config, graph, session), predecessor)
                           for predecessor in predecessors]
    return processor_fun(session, dependency_list)


class SparkDataPrepComposer(DataPrepComposer):

    def __init__(self, edges, config, spark_session):
        super().__init__(edges, config)
        self.spark_session = spark_session
        self.cache_map = {}

    def __call__(self):
        sink_nodes = [n for n, d in self.graph.out_degree() if d == 0]
        for node in sink_nodes:
            process_composition(node, self.config, graph=self.graph,
                                session=self.spark_session)
