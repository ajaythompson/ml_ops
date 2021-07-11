import os

import pytest
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession

from ml_ops.processor import ProcessorContext
from ml_ops.processor.batch import LoadProcessor

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


def test_load_processor(spark_session: SparkSession):
    load_options = {
        'header': 'true'
    }

    processor_context = ProcessorContext(spark_session)
    processor_context.set_property(LoadProcessor.PATH,
                                   f'{FIXTURE_DIR}/sample_load.csv')
    processor_context.set_property(LoadProcessor.FORMAT, 'csv')

    for key, value in load_options.items():
        processor_context.set_dynamic_property(f'read.{key}', value)

    processor = LoadProcessor()
    output = processor.run(processor_context)
    actual = output.df.collect()
    expected_data = [{'name': 'xyz', 'contact': '123'}]
    expected = spark_session.createDataFrame(expected_data) \
        .select('name', 'contact').collect()
    assert actual == expected
