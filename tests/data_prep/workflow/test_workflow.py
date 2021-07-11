import json
import os
import shutil

import pytest
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession

from ml_ops.workflow import InMemoryWFRepository, \
    Workflow
from ml_ops.workflow import SparkWorkflowManager

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'test_files',
)

TEST_DIR = f'{FIXTURE_DIR}/output'


@pytest.fixture
def spark():
    conf = SparkConf()
    conf.set("spark.master", "local[1]")
    conf.set("spark.app.name", "tests_helper")
    conf.set("spark.sql.shuffle.partitions", "3")
    if not os.path.exists(TEST_DIR):
        os.mkdir(TEST_DIR)
    yield SparkSession.builder.config(conf=conf).getOrCreate()
    shutil.rmtree(TEST_DIR, ignore_errors=True)


def test_workflow_serializer(spark: SparkSession):

    csv_options = {
        'header': 'true'
    }

    with open(f'{FIXTURE_DIR}/workflow.json') as f:
        content = f.read().replace('##FIXTURE_DIR##', FIXTURE_DIR)
        workflow_config = json.loads(content)

    workflow = Workflow.get_workflow(workflow_config)

    wf_repo = InMemoryWFRepository()
    wf_manager = SparkWorkflowManager(wf_repo)

    wf_repo.update_workflow(workflow.id, workflow)
    wf_manager.run(workflow.id, spark)

    actual = spark.read.format('csv').options(**csv_options) \
        .load(f'{TEST_DIR}/enriched_employee') \
        .collect()

    expected = spark.read.format('csv').options(**csv_options) \
        .load(f'{FIXTURE_DIR}/enriched_employee_output.csv') \
        .collect()

    assert actual == expected
