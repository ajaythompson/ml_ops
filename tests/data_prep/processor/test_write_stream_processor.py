import os
import shutil

import pytest
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from ml_ops.data_prep.processor import ProcessorContext, PropertyGroup, \
    PropertyGroups, Dependency
from ml_ops.data_prep.processor.stream import WriteStreamProcessor

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'test_files',
)

TEST_DIR = f'{FIXTURE_DIR}/output'


@pytest.fixture
def spark_session():
    conf = SparkConf()
    conf.set("spark.master", "local[1]")
    conf.set("spark.app.name", "tests_helper")
    conf.set("spark.sql.shuffle.partitions", "3")
    yield SparkSession.builder.config(conf=conf).getOrCreate()
    shutil.rmtree(TEST_DIR, ignore_errors=True)


def test_write_stream_processor(spark_session: SparkSession):
    write_options = {
        'header': 'true',
        'inferSchema': 'true',
        'checkpointLocation': f'{TEST_DIR}/checkpoint'
    }

    schema = {
        'type': 'struct',
        'fields': [
            {'name': 'name', 'type': 'string',
             'nullable': False, 'metadata': {}},
            {'name': 'contact', 'type': 'integer',
             'nullable': False, 'metadata': {}},
        ]
    }

    input_path = f'{FIXTURE_DIR}/sample_load.csv'
    output_path = f'{TEST_DIR}/sample_load.csv'
    default_props = PropertyGroup()
    default_props.set_property(WriteStreamProcessor.PATH, output_path)
    default_props.set_property(WriteStreamProcessor.FORMAT, 'csv')

    property_groups = PropertyGroups()
    property_groups.set_property_group(
        WriteStreamProcessor.WRITE_OPTIONS_GROUP, write_options)
    property_groups.set_property_group(
        WriteStreamProcessor.DEFAULT_PROPS_GROUP, default_props
    )

    dependency_df = spark_session.readStream.load(
        path=input_path,
        format='csv',
        schema=StructType.fromJson(schema),
        **write_options)
    dependency = Dependency(dependency_df, {})
    processor_context = ProcessorContext(spark_session,
                                         property_groups,
                                         [dependency])

    processor = WriteStreamProcessor()
    processor.run(processor_context)

    actual = spark_session \
        .read \
        .options(**write_options) \
        .csv(output_path) \
        .collect()

    expected = spark_session \
        .read \
        .options(**write_options) \
        .csv(input_path) \
        .collect()

    assert actual == expected
