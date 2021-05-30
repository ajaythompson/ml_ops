from __future__ import annotations
from abc import ABC, abstractmethod
from ml_ops.data_prep.processor.property import PropertyDescriptor, \
    PropertyDescriptorBuilder
from typing import Dict, List, Union
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession


class Dependency:

    def __init__(self, df: DataFrame, config: Dict = {}) -> None:
        self.df = df
        self.config = config


class SparkProcessor(ABC):
    """
        Base class for post processor.
    """

    _types = {}

    DEFAULT_PROPS_GROUP = 'default'

    @staticmethod
    def get_spark_processor(type: str) -> SparkProcessor:
        assert type in SparkProcessor._types, \
            f'SparkProcessor implementation not found ' \
            f'for {type}.'
        return SparkProcessor._types.get(type)()

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._types[cls.__name__] = cls

    @abstractmethod
    def get_property_groups(self):
        pass

    @abstractmethod
    def run(self,
            processor_context) -> Union[Dependency, None]:
        pass


class IncomingDependency(ABC):
    @abstractmethod
    def validate_dependency_count(self, dependencies):
        pass


class ZeroIncomingDependency(IncomingDependency):
    def validate_dependency_count(self, dependencies):
        if dependencies is None:
            dependencies = []
        return len(dependencies) == 0


class SingleIncomingDependency(IncomingDependency):
    def dependency_count(self):
        return 1


class MultiIncomingDependency(IncomingDependency):
    def dependency_count(self):
        return 1


class TransformProcessor(SparkProcessor):

    VIEW_NAME = PropertyDescriptorBuilder() \
        .name('view_name') \
        .description('Temp view name of the dependency.') \
        .property_group(SparkProcessor.DEFAULT_PROPS_GROUP) \
        .required(False) \
        .build()

    @abstractmethod
    def run(self, processor_context, spark_session) -> Dependency:
        pass


class ActionProcessor(SparkProcessor):

    @abstractmethod
    def run(self, processor_context, spark_session) -> None:
        pass


class ProcessorContext:

    def __init__(self,
                 spark_session: SparkSession,
                 property_groups: Dict = {},
                 dependencies: List[Dependency] = []) -> None:
        self.property_groups = property_groups
        self.dependencies = dependencies
        self.spark_session = spark_session

    def set_property(self,
                     property_descriptor: PropertyDescriptor,
                     value):
        property_group_name = property_descriptor.property_group
        property_name = property_descriptor.name
        if property_group_name not in self.property_groups:
            self.property_groups[property_group_name] = {}

        property_group = self.property_groups[property_group_name]
        property_group[property_name] = value

    def set_property_group(self,
                           property_group_name: str,
                           property_group_value: dict):
        self.property_groups[property_group_name] = property_group_value

    def get_property_group(self,
                           property_group):
        return self.property_groups.get(property_group, {})

    def get_property(self,
                     property_descriptor: PropertyDescriptor):
        property_group_name = property_descriptor.property_group
        property_name = property_descriptor.name
        property_dict = self.property_groups.get(property_group_name, {})
        return property_dict.get(property_name,
                                 property_descriptor.default_value)
