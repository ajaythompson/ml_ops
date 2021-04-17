from collections import Counter
from ml_ops.data_prep.workflow.processors.spark.processor \
 import Dependency, SparkProcessor
import networkx as nx
from networkx.classes.digraph import DiGraph
import itertools
import jsonschema


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
                        "required": ["left", "right"],
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
                proc_config['type'], proc_config['version'])
            processor.validate(proc_config)

        # check for duplicate processor names
        processor_names = [processor['name']
                           for processor in config['processors']]
        duplicate_names = [item for item, count in Counter(
            processor_names).items() if count > 1]
        assert len(duplicate_names) == 0, \
            f'Processor names in {duplicate_names} is repeated. ' \
            'Processors should have unique names.'

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

    def process_composition(
            self, node, config, graph: DiGraph, session) -> SparkProcessor:
        properties = config[node].get('properties', {})
        processor_name = config[node]['name']
        processor_type = config[node]['type']
        processor_version = config[node]['version']
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
            processor_type, processor_version)(
            session, processor_name, properties, dependencies)
        return processor

    def run(self):

        assert 'processors' in self.config, \
            'Missing "processors" in {self.config}!'

        processor_mandatory_keys = ['name', 'type']
        for processor, key in itertools.product(
                self.config['processors'], processor_mandatory_keys):
            assert key in processor, \
                f'Missing mandatory processor config "{key}" in {processor}!'

        sink_nodes = [n for n, d in self.graph.out_degree() if d == 0]
        processor_config_map = {processor['name']: processor
                                for processor in self.config['processors']}

        output_dfs = []
        for node in sink_nodes:
            processor = self.process_composition(node,
                                                 processor_config_map,
                                                 graph=self.graph,
                                                 session=self.spark)
            output_dfs.append(processor.run())
        return output_dfs
