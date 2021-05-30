from abc import ABC, abstractmethod
from typing import Union

from pyspark.sql.session import SparkSession
from ml_ops.data_prep.processor import ProcessorContext
from ml_ops.data_prep.processor import Dependency, SparkProcessor
import networkx as nx
from networkx.classes.digraph import DiGraph
import json
import uuid


class WFProcessor:

    def __init__(self,
                 name,
                 type,
                 property_groups):
        self.id = str(uuid.uuid1())
        self.name = name
        self.type = type
        self.property_groups = property_groups


class WFRelation:

    def __init__(self, left, right) -> None:
        self.id = str(uuid.uuid1())
        self.left = left
        self.right = right


class Workflow:

    def __init__(self) -> None:
        self.id = str(uuid.uuid1())
        self.processors = {}
        self.relations = {}

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
        wf_processor.id(processor_id)
        self.processors.update(processor_id, wf_processor)
        return self

    def update_relation(self, relation_id, wf_relation: WFRelation):
        wf_relation.id(wf_relation)
        self.relations.update(relation_id, wf_relation)
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

    @ abstractmethod
    def create_workflow(self) -> Workflow:
        """Adds a new workflow.

        Returns:
            str: workflow.
        """
        pass

    @ abstractmethod
    def read_workflow(self, id: str) -> Workflow:
        """Fetch the workflow for the given id.

        Args:
            id (str): workflow id.

        Returns:
            Workflow: workflow.
        """
        pass

    @ abstractmethod
    def update_workflow(self, id: str, workflow: Workflow) -> Workflow:
        """Updates the id with the given workflow.

        Args:
            id (str): Id of the workflow to be updated.
            workflow (Workflow): The updated workflow.

        Returns:
            Workflow: The updated workflow.
        """
        pass

    @ abstractmethod
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
        self.update(id, workflow)
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

    # def run(self, workflow_id, spark):

    #     workflow: Workflow = self.wf_repo.read_workflow(workflow_id)
    #     relations: WFRelation = workflow.relations

    #     graph = nx.DiGraph()
    #     edges = relations
    #     edges = [(relation.left, relation.right)
    #              for relation in relations]
    #     for processor in workflow.processors.values():
    #         graph.add_node(processor.id)
    #     graph.add_edges_from(edges)

    #     sink_nodes = [n for n, d in graph.out_degree() if d == 0]

    #     output_dfs = []
    #     for node in sink_nodes:
    #         processor = self.process_composition(node,
    #                                              self.processors_map,
    #                                              graph=graph,
    #                                              spark=spark)
    #         output_dfs.append(processor.run())
    #     return output_dfs

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
