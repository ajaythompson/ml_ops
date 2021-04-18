from ml_ops.data_prep.workflow import SparkWorkflowManager
import pytest
import os
import yaml
import shutil

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'test_files',
)

TEST_DIR = f'{FIXTURE_DIR}/output'


@pytest.fixture
def spark_session():
    conf = SparkConf()
    conf.set("spark.master", "local[4]")
    conf.set("spark.app.name", "tests_helper")
    conf.set("spark.sql.shuffle.partitions", "8")
    yield SparkSession.builder.config(conf=conf).getOrCreate()
    shutil.rmtree(TEST_DIR, ignore_errors=True)


def test_valid_spark_processor(spark_session):
    config = f"""
processors:
  - id: load_employee
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - id: write_enriched_employee
    type: write_processor
    version: v1
    properties:
        format: csv
        path: {FIXTURE_DIR}/output/enriched_employee
        options:
            header: 'true'

relations:
  - id: load_employee_2_join_write_enriched_employee
    left: load_employee
    right: write_enriched_employee
"""

    SparkWorkflowManager.get_workflow_manager(yaml.load(config)) \
        .run(spark=spark_session)
    expected = spark_session.read.option('header', True).csv(
        f'{FIXTURE_DIR}/employee.csv').collect()
    actual = spark_session.read.option('header', True).csv(
        f'{FIXTURE_DIR}/output/enriched_employee').collect()
    assert expected == actual
