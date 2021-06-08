from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from ml_ops.data_prep.processor import ProcessorContext, \
    PropertyGroup, PropertyGroups
from ml_ops.data_prep.processor.batch import LoadProcessor
import pytest
import os

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

    default_props = PropertyGroup()
    default_props.set_property(
        LoadProcessor.PATH, f'{FIXTURE_DIR}/sample_load.csv')
    default_props.set_property(LoadProcessor.FORMAT, 'csv')

    property_groups = PropertyGroups()
    property_groups.set_property_group(
        LoadProcessor.LOAD_OPTIONS_GROUP, load_options)
    property_groups.set_property_group(
        LoadProcessor.DEFAULT_PROPS_GROUP, default_props
    )

    processor_context = ProcessorContext(spark_session)
    processor_context.set_property_group(
        LoadProcessor.LOAD_OPTIONS_GROUP, load_options)
    processor_context.set_property_group(
        LoadProcessor.DEFAULT_PROPS_GROUP, default_props
    )
    processor = LoadProcessor()
    output = processor.run(processor_context)
    actual = output.df.collect()
    expected_data = [{'name': 'xyz', 'contact': '123'}]
    expected = spark_session.createDataFrame(expected_data) \
        .select('name', 'contact').collect()
    assert actual == expected
