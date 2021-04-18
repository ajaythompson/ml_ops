from collections import Counter
from functools import singledispatch
from typing import Dict
from ml_ops.data_prep.workflow.processors.spark.processor \
 import Dependency, SparkProcessor
import networkx as nx
from networkx.classes.digraph import DiGraph
import json
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
                            'id': {'type': 'string'},
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

        # check for duplicate processor ids
        processor_ids = [processor['id'] for processor in config['processors']]
        duplicate_ids = [item for item, count in Counter(
            processor_ids).items() if count > 1]
        assert len(duplicate_ids) == 0, \
            f'Processor ids in {duplicate_ids} is repeated. ' \
            'Processors should have unique id.'

    def __init__(self, processors=[], relations=[]) -> None:
        super().__init__()
        self.df_map = {}
        self.processors_map = {processor['id']: processor
                               for processor in processors}
        self.relations_map = {relation['id']: relation
                              for relation in relations}

    def add_processor(self, processor) -> None:
        self.processors_map[processor['id']] = processor

    def get_config(self):
        config = {}
        config['processors'] = list(self.processors_map.values())
        config['relations'] = list(self.relations_map.values())
        return config

    @singledispatch
    def get_workflow_manager(config: Dict):
        SparkWorkflowManager.validate(config)
        processors = config['processors']
        relations = config['relations']
        return SparkWorkflowManager(processors=processors,
                                    relations=relations)

    def process_composition(
            self, node, config, graph: DiGraph, spark) -> SparkProcessor:
        properties = config[node].get('properties', {})
        processor_id = config[node]['id']
        processor_type = config[node]['type']
        processor_version = config[node]['version']
        predecessors = list(graph.predecessors(node))
        dependencies = []
        if not bool(predecessors):
            predecessors = []
        for predecessor in predecessors:
            prev_processor = self.process_composition(
                predecessor, config, graph, spark)
            try:
                df = self.df_map.get(predecessor, prev_processor.run())
            except Exception as error:
                raise error
            dependencies.append(Dependency(df, predecessor))
        processor = SparkProcessor.get_spark_processor(
            processor_type, processor_version)(
            spark, processor_id, properties, dependencies)
        return processor

    def run(self, spark):
        config = {}
        config['processors'] = list(self.processors_map.values())
        config['relations'] = list(self.relations_map.values())
        self.validate(config)

        graph = nx.DiGraph()
        edges = [(relation['left'], relation['right'])
                 for relation in config['relations']]
        for processor in config['processors']:
            graph.add_node(processor['id'])
        graph.add_edges_from(edges)

        sink_nodes = [n for n, d in graph.out_degree() if d == 0]

        output_dfs = []
        for node in sink_nodes:
            processor = self.process_composition(node,
                                                 self.processors_map,
                                                 graph=graph,
                                                 spark=spark)
            output_dfs.append(processor.run())
        return output_dfs

    def show_json(self, node, limit, spark):
        config = {}
        config['processors'] = list(self.processors_map.values())
        config['relations'] = list(self.relations_map.values())
        self.validate(config)
        graph = nx.DiGraph()
        edges = [(relation['left'], relation['right'])
                 for relation in config['relations']]
        for processor in config['processors']:
            graph.add_node(processor['id'])
        graph.add_edges_from(edges)

        processor = self.process_composition(node,
                                             self.processors_map,
                                             graph=graph,
                                             spark=spark)
        result = [json.loads(_) for _ in
                  processor.run().limit(limit).toJSON().collect()]
        return result
