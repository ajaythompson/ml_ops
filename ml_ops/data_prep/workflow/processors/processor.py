from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
import jsonschema


@dataclass
class Dependency:
    df: DataFrame
    alias: str


class SparkProcessor(ABC):
    """
        Base class for post processor.
    """

    _types = {}

    @staticmethod
    def get_spark_processor(type: str, version: str):
        assert type in SparkProcessor._types and \
            version in SparkProcessor._types[type], \
            f'SparkProcessor implementation not found ' \
            f'for {type} with version {version}.'
        return SparkProcessor._types[type][version]

    def __init__(self, spark: SparkSession, id: str, properties: Dict = {
    }, dependencies: List[Dependency] = []) -> None:
        super().__init__()
        self.spark = spark
        self.id = id
        self.properties = properties
        missing_properties = list(
            set(self.mandatory_properties).difference(properties.keys()))
        assert not bool(missing_properties), \
            f'Missing mandatory properties {missing_properties} ' \
            f'for processor {self.name}!'
        self.dependencies = dependencies

    @classmethod
    def __init_subclass__(cls, type, version, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._types[type] = {version: cls}

    @abstractmethod
    def get_property_groups(self):
        pass

    @abstractmethod
    def run(self) -> DataFrame:
        pass

    def select(fun):
        def select_inner(self):
            df = fun(self)
            if 'select_columns' in self.properties:
                df = df.selectExpr(self.properties['select_columns'])
            return df
        return select_inner

    def filter(fun):
        def filter_inner(self):
            df = fun(self)
            if 'filter_condition' in self.properties:
                df = df.filter(self.properties['filter_condition'])
            return df
        return filter_inner

    def limit(fun):
        def limit_inner(self):
            df = fun(self)
            if 'limit' in self.properties:
                df = df.limit(self.properties['limit'])
            return df
        return limit_inner

class ProcessorContext(ABC):

    def get_property(property_group, property_descriptor):
        return 