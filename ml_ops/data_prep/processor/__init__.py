from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Union

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from ml_ops.data_prep.processor.property import PropertyGroup, PropertyGroups
from ml_ops.data_prep.processor.property import PropertyDescriptor, \
    PropertyDescriptorBuilder, PropertyGroupDescriptor, PropertyGroup, \
    PropertyGroups


class Dependency:

    def __init__(self, df: DataFrame, config: Dict) -> None:
        self.df = df
        self.config = config


class SparkProcessor(ABC):
    """
        Base class for post processor.
    """

    _types = {}

    DEFAULT_PROPS_GROUP = 'default'

    @staticmethod
    def get_spark_processor(processor_type: str) -> SparkProcessor:
        assert processor_type in SparkProcessor._types, \
            f'SparkProcessor implementation not found ' \
            f'for {processor_type}.'
        return SparkProcessor._types.get(processor_type)()

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._types[cls.__name__] = cls

    @abstractmethod
    def get_property_groups(self) -> List[PropertyGroupDescriptor]:
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
        .required(False) \
        .build()

    @abstractmethod
    def run(self, processor_context) -> Dependency:
        pass


class ActionProcessor(SparkProcessor):

    @abstractmethod
    def run(self, processor_context) -> None:
        pass


class ProcessorContext:

    def __init__(self,
                 spark_session: SparkSession,
                 property_groups: PropertyGroups = None,
                 dependencies: List[Dependency] = None) -> None:
        if property_groups is None:
            property_groups = PropertyGroups()

        self.property_groups = property_groups
        self.dependencies = dependencies
        self.spark_session = spark_session

    def set_property_group(
            self,
            property_group_descriptor: PropertyGroupDescriptor,
            property_group: PropertyGroup):
        self.property_groups.set_property_group(
            property_group_descriptor, property_group)

    def get_property_group(
            self,
            property_group_descriptor: PropertyGroupDescriptor
    ) -> PropertyGroup:
        return self.property_groups.get_property_group(
            property_group_descriptor)

    def get_logger(self):
        log4j = self.spark_session._jvm.org.apache.log4j.Logger
        return log4j.getLogger(self.__class__.__name__)
