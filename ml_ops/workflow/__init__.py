from __future__ import annotations

import json
import uuid
from abc import ABC, abstractmethod
from typing import Union

import networkx as nx
from networkx.classes.digraph import DiGraph
from pyspark.sql.session import SparkSession

from ml_ops.processor import Dependency, SparkProcessor, \
    PropertyGroups, PropertyGroup
from ml_ops.processor import ProcessorContext


class WorkflowConfigException(Exception):
    pass


class WFProcessor:

    def __init__(self,
                 name,
                 type,
                 property_groups,
                 id=None):
        if id is None:
            id = str(uuid.uuid1())
        self.id = id
        self.name = name
        self.type = type
        self.property_groups = property_groups

    def json_value(self):
        json_value = {
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'property_groups': self.property_groups
        }
        return json_value

    @classmethod
    def get_processor(cls, config: dict) -> WFProcessor:
        processor_id = config.get('id')
        name = config.get('name', '')
        type = config.get('type')
        property_groups_config = config.get('property_groups', {})

        if processor_id is None:
            raise WorkflowConfigException(f'Missing processor id in config.')

        if type is None:
            raise WorkflowConfigException(
                'Missing processor type in processor',
                f' config with id {processor_id}.'
            )

        property_groups = PropertyGroups()

        for group_name, group_properties in property_groups_config.items():
            property_group = PropertyGroup()
            for property_name, property_value in group_properties.items():
                property_group[property_name] = property_value

            property_groups[group_name] = property_group

        return WFProcessor(
            name=name,
            type=type,
            property_groups=property_groups,
            id=processor_id
        )


class WFRelation:

    def __init__(self, left, right, id=None) -> None:
        if id is None:
            id = str(uuid.uuid1())
        self.id = id
        self.left = left
        self.right = right

    def json_value(self):
        json_value = {
            'id': self.id,
            'left': self.left,
            'right': self.right
        }
        return json_value

    @classmethod
    def get_relation(cls, config: dict) -> WFRelation:
        relation_id = config.get('id')
        left = config.get('left')
        right = config.get('right')

        if relation_id is None:
            raise WorkflowConfigException(f'Missing relation id in config.')

        if left is None:
            raise WorkflowConfigException(
                f'Missing left relation id in config.'
                f'for the relation {relation_id}.')

        if right is None:
            raise WorkflowConfigException(
                f'Missing right relation id in config.'
                f'for the relation {relation_id}.')

        return WFRelation(left, right, relation_id)


class Workflow:

    def __init__(self, id=None, processors=None, relations=None) -> None:
        if id is None:
            id = str(uuid.uuid1())

        if processors is None:
            processors = {}

        if relations is None:
            relations = {}

        self.id = id
        self.processors = processors
        self.relations = relations

    @classmethod
    def get_workflow(cls, config: dict) -> Workflow:
        workflow_id = config.get('id')
        if workflow_id is None:
            raise WorkflowConfigException(f'Missing workflow id in config.')
        processors = config.get('processors', [])
        relations = config.get('relations', [])

        processor_list = [WFProcessor.get_processor(processor_config) for
                          processor_config in processors]
        relation_list = [WFRelation.get_relation(relation_config) for
                         relation_config in relations]

        processor_dict = {processor.id: processor
                          for processor in processor_list}
        relation_dict = {relation.id: relation
                         for relation in relation_list}

        return Workflow(id, processor_dict, relation_dict)

    def json_value(self):

        json_processors = [processor.json_value()
                           for processor in self.processors.values()]

        json_relations = [relation.json_value()
                          for relation in self.relations.values()]

        json_value = {
            'id': self.id,
            'processors': json_processors,
            'relations': json_relations
        }
        return json_value

    def add_processor(self, wf_processor: WFProcessor):
        self.processors[wf_processor.id] = wf_processor
        return self

    def add_relation(self, wf_relation: WFRelation):
        self.relations[wf_relation.id] = wf_relation
        return self

    def get_processor(self, processor_id) -> WFProcessor:
        assert processor_id in self.processors, \
            f'Processor with id {processor_id} not found.'
        return self.processors[processor_id]

    def get_relation(self, relation_id):
        return self.relations.get(relation_id)

    def update_processor(self, processor_id, wf_processor: WFProcessor):
        wf_processor.id = processor_id
        self.processors[processor_id] = wf_processor
        return self

    def update_relation(self, relation_id, wf_relation: WFRelation):
        wf_relation.id = wf_relation
        self.processors[relation_id] = wf_relation
        return self

    def remove_processor(self, processor_id):
        self.processors.pop(processor_id)
        return self

    def get_graph(self):
        graph = nx.DiGraph()
        edges = [(relation.left, relation.right)
                 for relation in self.relations.values()]
        for processor in self.processors.values():
            graph.add_node(processor.id)
            graph.add_edges_from(edges)
        return graph

    @classmethod
    def get_end_processors(cls, graph):
        return [n for n, d in graph.out_degree() if d == 0]


class WorkflowRepository(ABC):

    @abstractmethod
    def create_workflow(self) -> Workflow:
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
                       spark: SparkSession) -> Union[Dependency, None]:
        processor_config = workflow.get_processor(processor_id)
        predecessors = []
        if graph.number_of_edges() > 0:
            predecessors = list(graph.predecessors(processor_id))
        dependencies = []
        if not bool(predecessors):
            predecessors = []
        for predecessor in predecessors:
            dependencies.append(
                cls.get_dependency(workflow, predecessor, graph, spark))

        processor_context = ProcessorContext(
            spark_session=spark,
            property_groups=processor_config.property_groups,
            dependencies=dependencies)
        processor = SparkProcessor.get_spark_processor(processor_config.type)

        return processor.run(processor_context)

    # @classmethod
    # def validate_processor(cls, wf_processor: WFProcessor):
    #     actual = wf_processor.property_groups
    #     processor_type = wf_processor.type
    #     processor = SparkProcessor.get_spark_processor(processor_type)
    #     expected = processor.get_property_groups()

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
