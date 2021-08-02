from __future__ import annotations

import json
import uuid
from abc import ABC, abstractmethod
from typing import List, Union

import networkx as nx
from networkx.classes.digraph import DiGraph
from pyspark.sql.session import SparkSession

from ml_ops.processor import FlowDF, SparkProcessor
from ml_ops.processor import ProcessorContext


class WorkflowConfigException(Exception):
    pass


class ProcessorConfig:

    def __init__(self,
                 name,
                 processor_type,
                 properties,
                 processor_id=None):
        if processor_id is None:
            processor_id = str(uuid.uuid1())
        self.processor_id = processor_id
        self.name = name
        self.processor_type = processor_type
        self.properties = properties

    def json_value(self):
        json_value = {
            'id': self.processor_id,
            'name': self.name,
            'type': self.processor_type,
            'properties': self.properties
        }
        return json_value

    @classmethod
    def get_processor(cls, config: dict) -> ProcessorConfig:
        processor_id = config.get('id', str(uuid.uuid1()))
        name = config.get('name', '')
        processor_type = config.get('type')
        properties = config.get('properties', {})

        if processor_id is None:
            raise WorkflowConfigException('Missing processor id in config.')

        if processor_type is None:
            raise WorkflowConfigException(
                'Missing processor type in processor',
                f' config with id {processor_id}.'
            )

        return ProcessorConfig(
            name=name,
            processor_type=processor_type,
            properties=properties,
            processor_id=processor_id
        )


class ConnectionConfig:

    def __init__(self, left, right, connection_id=None, relation=None) -> None:
        if connection_id is None:
            connection_id = str(uuid.uuid1())
        self.connection_id = connection_id
        self.left = left
        self.right = right
        self.relation = relation

    def json_value(self):
        json_value = {
            'id': self.connection_id,
            'left': self.left,
            'right': self.right
        }

        if self.relation is not None:
            json_value['relation'] = self.relation

        return json_value

    @classmethod
    def get_relation(cls, config: dict) -> ConnectionConfig:
        relation_id = config.get('id', str(uuid.uuid1()))
        left = config.get('left')
        right = config.get('right')
        relation = config.get('relation')

        if relation_id is None:
            raise WorkflowConfigException('Missing relation id in config.')

        if left is None:
            raise WorkflowConfigException(
                f'Missing left relation id in config.'
                f'for the relation {relation_id}.')

        if right is None:
            raise WorkflowConfigException(
                f'Missing right relation id in config.'
                f'for the relation {relation_id}.')

        return ConnectionConfig(left, right, relation_id, relation)


class Workflow:

    def __init__(self, id=None, processors=None, connections=None) -> None:
        if id is None:
            id = str(uuid.uuid1())

        if processors is None:
            processors = {}

        if connections is None:
            connections = {}

        self.id = id
        self.processors = processors
        self.connections = connections

    @classmethod
    def get_workflow(cls, config: dict) -> Workflow:
        workflow_id = config.get('id')
        if workflow_id is None:
            raise WorkflowConfigException('Missing workflow id in config.')
        processors = config.get('processors', [])
        relations = config.get('relations', [])

        processor_list = [ProcessorConfig.get_processor(processor_config) for
                          processor_config in processors]
        relation_list = [ConnectionConfig.get_relation(relation_config) for
                         relation_config in relations]

        processor_dict = {processor.processor_id: processor
                          for processor in processor_list}
        relation_dict = {relation.connection_id: relation
                         for relation in relation_list}

        return Workflow(id, processor_dict, relation_dict)

    def json_value(self):

        json_processors = [processor.json_value()
                           for processor in self.processors.values()]

        json_relations = [relation.json_value()
                          for relation in self.connections.values()]

        json_value = {
            'id': self.id,
            'processors': json_processors,
            'relations': json_relations
        }
        return json_value

    def add_processor(self, wf_processor: ProcessorConfig):
        self.processors[wf_processor.processor_id] = wf_processor
        return self

    def add_relation(self, wf_relation: ConnectionConfig):
        self.connections[wf_relation.connection_id] = wf_relation
        return self

    def get_processor(self, processor_id) -> ProcessorConfig:
        assert processor_id in self.processors, \
            f'Processor with id {processor_id} not found.'
        return self.processors[processor_id]

    def get_relation(self, relation_id):
        return self.connections.get(relation_id)

    def update_processor(self, processor_id, wf_processor: ProcessorConfig):
        wf_processor.id = processor_id
        self.processors[processor_id] = wf_processor
        return self

    def update_relation(self, relation_id, wf_relation: ConnectionConfig):
        wf_relation.id = wf_relation
        self.processors[relation_id] = wf_relation
        return self

    def remove_processor(self, processor_id):
        self.processors.pop(processor_id)
        return self

    def get_graph(self):
        graph = nx.DiGraph()
        # edges = [(relation.left, relation.right)
        #          for relation in self.connections.values()]

        for connection in self.connections.values():
            graph.add_edge(connection.left, connection.right,
                           relation=connection.relation)
        for processor in self.processors.values():
            graph.add_node(processor.processor_id)
            # graph.add_edges_from(edges)
        return graph

    @classmethod
    def get_end_processors(cls, graph):
        return [n for n, d in graph.out_degree() if d == 0]


class WorkflowRepository(ABC):

    _types = {}

    @classmethod
    def get_repository(cls, repository_type: str) -> WorkflowRepository:
        assert repository_type in WorkflowRepository._types, \
            f'Workflow repository implementation not found ' \
            f'for {repository_type}.'
        return WorkflowRepository._types.get(repository_type)()

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._types[cls.__name__] = cls

    @abstractmethod
    def create_workflow(self) -> Workflow:
        """Adds a new workflow.

        Returns:
            str: workflow.
        """
        pass

    @abstractmethod
    def get_workflows(self) -> List[Workflow]:
        """Adds a new workflow.

        Returns:
            str: workflow.
        """
        pass

    @abstractmethod
    def read_workflow(self, id: str) -> Workflow:
        """Fetch the workflow for the given id.

        Args:
            id (str): workflow id.

        Returns:
            Workflow: workflow.
        """
        pass

    @abstractmethod
    def update_workflow(self, id: str, workflow: Workflow) -> Workflow:
        """Updates the id with the given workflow.

        Args:
            id (str): Id of the workflow to be updated.
            workflow (Workflow): The updated workflow.

        Returns:
            Workflow: The updated workflow.
        """
        pass

    @abstractmethod
    def delete_workflow(self, id: str) -> None:
        """Delete the workflow for the given id.

        Args:
            id (str): id of the workflow to be deleted.

        """
        pass


class InMemoryWFRepository(WorkflowRepository, dict):

    def create_workflow(self) -> Workflow:
        workflow = Workflow()
        self[workflow.id] = workflow
        return workflow

    def get_workflows(self) -> List[Workflow]:
        return self.values()

    def read_workflow(self, id: str) -> Workflow:
        return self.get(id)

    def update_workflow(self, id: str, workflow: Workflow) -> Workflow:
        workflow.id = id
        self[id] = workflow
        return self.read_workflow(id)

    def delete_workflow(self, id: str) -> None:
        self.pop(id)


class SparkWorkflowManager:

    def __init__(self, wf_repo: WorkflowRepository) -> None:
        self.wf_repo = wf_repo

    def create_workflow(self) -> Workflow:
        return self.wf_repo.create_workflow()

    def read_workflow(self, id: str) -> Workflow:
        workflow = self.wf_repo.read_workflow(id)
        assert workflow is not None, \
            'Failed to find workflow with id {id}.'
        return workflow

    def update_workflow(self, id: str, workflow: Workflow) -> Workflow:
        return self.update_workflow(id, workflow)

    def delete_workflow(self, id: str) -> None:
        self.wf_repo.delete_workflow(id)

    def add_processor(self, wf_id: str, wf_processor) -> None:
        workflow = self.read_workflow(wf_id)
        workflow.add_processor(wf_processor)
        return workflow

    @classmethod
    def get_dependency(cls,
                       workflow: Workflow,
                       processor_id: str,
                       graph: DiGraph,
                       spark: SparkSession) -> Union[FlowDF, None]:
        processor_config = workflow.get_processor(processor_id)
        predecessors = []
        if graph.number_of_edges() > 0:
            predecessors = list(graph.predecessors(processor_id))
        if not bool(predecessors):
            predecessors = []

        processor_context = ProcessorContext(
            spark_session=spark,
            properties=processor_config.properties)
        for predecessor in predecessors:
            edge_data = graph.get_edge_data(predecessor, processor_id)
            relation = edge_data.get('relation')
            flow_df = cls.get_dependency(workflow, predecessor, graph, spark)
            if relation is not None:
                processor_context.set_dynamic_flow_df(relation, flow_df)
            else:
                processor_context.set_dynamic_flow_df(predecessor, flow_df)

        processor = SparkProcessor.get_spark_processor(
            processor_config.processor_type)

        return processor.run(processor_context)

    def run(self,
            workflow_id: str,
            spark: SparkSession,
            processor_id: str = None):

        workflow = self.read_workflow(workflow_id)
        graph = workflow.get_graph()
        sink_processor_ids = workflow.get_end_processors(graph)
        if processor_id is not None:
            sink_processor_ids = [processor_id]

        for processor_id in sink_processor_ids:
            self.get_dependency(workflow, processor_id, graph, spark)

    def show_json(self,
                  workflow_id: str,
                  processor_id: str,
                  spark,
                  limit=10):
        workflow = self.read_workflow(workflow_id)
        graph = workflow.get_graph()
        dependency = self.get_dependency(workflow, processor_id, graph, spark)
        df = dependency.df
        json_result = df.limit(limit).toJSON().collect()
        result = [json.loads(_) for _ in json_result]
        return result
