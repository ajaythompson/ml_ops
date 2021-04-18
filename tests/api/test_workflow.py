from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
import pytest
import json
import os
import yaml
import shutil
from ml_ops.api import workflow

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'test_files',
)


TEST_DIR = f'{FIXTURE_DIR}/output'


@pytest.fixture
def client():
    workflow.app.config['TESTING'] = True

    with workflow.app.test_client() as client:
        yield client
    shutil.rmtree(TEST_DIR, ignore_errors=True)


@pytest.fixture
def spark_session():
    conf = SparkConf()
    conf.set("spark.master", "local[4]")
    conf.set("spark.app.name", "tests_helper")
    conf.set("spark.sql.shuffle.partitions", "8")
    yield SparkSession.builder.config(conf=conf).getOrCreate()


def test_valid_processor(client):
    client.post(workflow.WORKFLOW_BASE_PATH)
    yaml_string = f"""
id: load_employee
type: load_processor
version: v1
properties:
    format: csv
    options:
        header: 'true'
    path: {FIXTURE_DIR}/employee.csv
        """
    client.post(workflow.PROCESSOR_BASE_PATH, json=yaml.load(yaml_string))
    rv = client.post(f'{workflow.WORKFLOW_PREVIEW_PATH}')
    actual = rv.json
    with open(f'{FIXTURE_DIR}/workflow_output.json') as f:
        expected = json.load(f)
    assert actual == expected


def test_valid_relation(client, spark_session):
    client.post(workflow.WORKFLOW_BASE_PATH)
    processor_config = {
        'load_employee': {
            'id': 'load_employee',
            'type': 'load_processor',
            'version': 'v1',
            'properties': {
                'format': 'csv',
                'options': {
                    'header': 'true'
                },
                'path': f'{FIXTURE_DIR}/employee.csv',
            }
        },
        'write_employee': {
            'id': 'write_employee',
            'type': 'write_processor',
            'version': 'v1',
            'properties': {
                'format': 'csv',
                'options': {
                    'header': 'true'
                },
                'path': f'{TEST_DIR}/employee.csv',
            }
        }
    }
    relation_config = {
        'write_employee': {
            'id': 'write_employee',
            'left': 'load_employee',
            'right': 'write_employee'
        }
    }
    client.post(workflow.PROCESSOR_BASE_PATH,
                json=processor_config['load_employee'])
    client.post(workflow.PROCESSOR_BASE_PATH,
                json=processor_config['write_employee'])
    client.post(workflow.RELATION_BASE_PATH,
                json=relation_config['write_employee'])
    client.post(workflow.WORKFLOW_RUN_PATH)
    expected = spark_session.read.option('header', True).csv(
        f'{FIXTURE_DIR}/employee.csv').collect()
    actual = spark_session.read.option('header', True).csv(
        f'{TEST_DIR}/employee.csv').collect()
    assert actual == expected
