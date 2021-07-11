import json
import os
import shutil

import pytest
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession

from ml_ops.processor import ProcessorContext
from ml_ops.processor.stream import LoadStreamProcessor

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

    processor_context = ProcessorContext(spark_session)
    processor_context.set_property(LoadStreamProcessor.PATH,
                                   f'{FIXTURE_DIR}/sample_load.csv')
    processor_context.set_property(LoadStreamProcessor.FORMAT, 'csv')
    processor_context.set_property(LoadStreamProcessor.SCHEMA,
                                   json.dumps(schema))

    for key, value in load_options.items():
        processor_context.set_dynamic_property(f'read.{key}', value)

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
