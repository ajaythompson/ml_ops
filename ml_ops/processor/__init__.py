from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Union

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from ml_ops.processor.property import PropertyDescriptor, \
    PropertyDescriptorBuilder
from ml_ops.processor.relation import RelationDescriptor


class FlowDF:

    def __init__(self, df: DataFrame, attributes: Dict) -> None:
        self.df = df
        self.attributes = attributes


class SparkProcessor(ABC):
    """
        Base class for post processor.
    """

    types = {}

    DEFAULT_PROPS_GROUP = 'default'

    @staticmethod
    def get_spark_processor(processor_type: str) -> SparkProcessor:
        assert processor_type in SparkProcessor.types, \
            f'SparkProcessor implementation not found ' \
            f'for {processor_type}.'
        return SparkProcessor.types.get(processor_type)()

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.types[cls.__name__] = cls

    @abstractmethod
    def get_property_descriptors(self) -> List[PropertyDescriptor]:
        pass

    @abstractmethod
    def get_relations(self) -> List[RelationDescriptor]:
        pass

    @abstractmethod
    def run(self,
            processor_context) -> Union[FlowDF, None]:
        pass

    def to_json(self) -> dict:
        result = {}
        result['type'] = self.__class__.__name__
        result['properties'] = [
            x.__dict__ for x in self.get_property_descriptors()]
        return result


class TransformProcessor:

    VIEW_NAME = PropertyDescriptorBuilder() \
        .name('view_name') \
        .description('Temp view name of the dependency.') \
        .required(False) \
        .build()

    @abstractmethod
    def run(self, processor_context) -> FlowDF:
        pass


class ActionProcessor:

    @abstractmethod
    def run(self, processor_context) -> None:
        pass


class ProcessorContext:

    def __init__(self,
                 spark_session: SparkSession,
                 properties: Dict = None,
                 flow_dfs: Dict[str, FlowDF] = None) -> None:
        if properties is None:
            properties = {}

        if flow_dfs is None:
            flow_dfs = {}

        self.properties = properties
        self.flow_dfs = flow_dfs
        self.spark_session = spark_session

    def get_properties(self):
        return self.properties

    def set_property(self,
                     property_descriptor: PropertyDescriptor,
                     value):
        self.properties[property_descriptor.name] = value

    def set_dynamic_property(self,
                             key: str,
                             value):
        self.properties[key] = value

    def ge_dynamic_property(self,
                            key: str):
        self.properties.get(key)

    def get_property(self,
                     property_descriptor: PropertyDescriptor,
                     default_value=None):
        property_name = property_descriptor.name
        return self.properties.get(property_name, default_value)

    def set_flow_df(self,
                    relation_descriptor: RelationDescriptor,
                    flow_df: FlowDF):
        relation_name = relation_descriptor.name
        self.flow_dfs[relation_name] = flow_df

    def set_dynamic_flow_df(self,
                            relation: str,
                            flow_df: FlowDF):
        self.flow_dfs[relation] = flow_df

    def get_flow_df(self, relation_descriptor: RelationDescriptor):
        relation_name = relation_descriptor.name
        return self.flow_dfs.get(relation_name)

    def get_flow_dfs(self):
        return self.flow_dfs.values()

    def get_logger(self):
        log4j = self.spark_session._jvm.org.apache.log4j.Logger
        return log4j.getLogger(self.__class__.__name__)
