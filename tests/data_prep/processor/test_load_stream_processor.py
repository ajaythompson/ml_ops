import json

from ml_ops.data_prep.processor.stream import LoadStreamProcessor
from ml_ops.data_prep.processor import ProcessorContext, PropertyGroup, \
    PropertyGroups
import os
import shutil

import pytest
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession

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
    conf.set("spark.sql.streaming.schemaInference ", "true")
    yield SparkSession.builder.config(conf=conf).getOrCreate()
    shutil.rmtree(TEST_DIR, ignore_errors=True)


def test_load_stream_processor(spark_session: SparkSession):

    schema = {
        'type': 'struct',
        'fields': [
            {'name': 'name',  'type': 'string',
                'nullable': False, 'metadata': {}},
            {'name': 'contact',  'type': 'integer',
                'nullable': False, 'metadata': {}},
        ]
    }

    load_options = {
        'header': 'true',
        'inferSchema': 'true',
        'checkpointLocation': f'{TEST_DIR}/checkpoint'
    }

    default_props = PropertyGroup()
    default_props.set_property(LoadStreamProcessor.PATH,
                               f'{FIXTURE_DIR}/sample_load.csv')
    default_props.set_property(LoadStreamProcessor.FORMAT, 'csv')
    default_props.set_property(LoadStreamProcessor.SCHEMA,
                               json.dumps(schema))

    property_groups = PropertyGroups()
    property_groups.set_property_group(
        LoadStreamProcessor.LOAD_OPTIONS_GROUP, load_options)
    property_groups.set_property_group(
        LoadStreamProcessor.DEFAULT_PROPS_GROUP, default_props
    )

    processor_context = ProcessorContext(
        spark_session, property_groups=property_groups)

    processor = LoadStreamProcessor()
    output = processor.run(processor_context)
    output_dir = f'{TEST_DIR}/stream_output'
    output.df.createOrReplaceTempView('input')
    output.df.writeStream.trigger(once=True) \
        .start(path=output_dir,
               format='csv',
               outputMode='append',
               **load_options) \
        .awaitTermination()

    actual = spark_session.read.options(**load_options) \
        .csv(output_dir).collect()
    expected_data = [{'name': 'xyz', 'contact': 123}]
    expected = spark_session.createDataFrame(
        expected_data).select('name', 'contact').collect()
    assert actual == expected
