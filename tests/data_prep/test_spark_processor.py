from ml_ops.data_prep.sparkprocessor import SparkWorkflowManager
import pytest
import os
import yaml
import shutil

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as f

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
  - name: load_employee
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - name: load_department
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/department.csv
  - name: join_employee_department
    type: join_processor
    version: v1
    properties:
      filter_condition: dept_id == 1
      storage_level: MEMORY_ONLY
      select_columns:
        - emp_id
        - name
        - dept
      'on':
      - dept_id
  - name: write_enriched_employee
    type: write_processor
    version: v1
    properties:
        format: csv
        path: {FIXTURE_DIR}/output/enriched_employee
        options:
            header: 'true'

relations:
  - left: load_employee
    right: join_employee_department
  - left: load_department
    right: join_employee_department
  - left: join_employee_department
    right: write_enriched_employee
"""

    SparkWorkflowManager(config=yaml.load(config), spark=spark_session).run()
    expected = spark_session.read.option('header', True).csv(
        f'{FIXTURE_DIR}/enriched_employee_output.csv').collect()
    actual = spark_session.read.option('header', True).csv(
        f'{FIXTURE_DIR}/output/enriched_employee').collect()
    assert expected == actual


def test_valid_spark_sql_processor(spark_session):
    config = f"""
processors:
  - name: load_employee
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
    type: load_processor
    version: v1
  - name: filter_employee
    properties:
      query: >
        SELECT emp_id, name, age, dept_id
        FROM load_employee WHERE dept_id == 1
    type: sql_processor
    version: v1
  - name: write_filter_employee
    type: write_processor
    version: v1
    properties:
        format: csv
        path: {FIXTURE_DIR}/output/sql_output
        options:
            header: 'true'
relations:
  - left: load_employee
    right: filter_employee
  - left: filter_employee
    right: write_filter_employee
"""

    SparkWorkflowManager(config=yaml.load(config), spark=spark_session).run()
    expected = spark_session.read.option('header', True).csv(
        f'{FIXTURE_DIR}/sql_output.csv').collect()
    actual = spark_session.read.option('header', True).csv(
        f'{FIXTURE_DIR}/output/sql_output').collect()
    assert expected == actual


def test_valid_spark_repartition_processor(spark_session):
    config = f"""
processors:
  - name: load_employee
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - name: repartition_employee
    type: repartition_processor
    version: v1
    properties:
      repartition:
        num_partitions: 5
relations:
  - left: load_employee
    right: repartition_employee
"""

    dfs = SparkWorkflowManager(config=yaml.load(
        config), spark=spark_session).run()

    assert dfs[0].rdd.getNumPartitions() == 5


def test_valid_spark_coalesce_processor(spark_session):
    config = f"""
processors:
  - name: load_employee
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - name: repartition_employee
    type: repartition_processor
    version: v1
    properties:
      repartition:
        num_partitions: 8
  - name: coalesce_employee
    type: repartition_processor
    version: v1
    properties:
      coalesce:
        num_partitions: 5
relations:
  - left: load_employee
    right: repartition_employee
  - left: repartition_employee
    right: coalesce_employee
"""

    dfs = SparkWorkflowManager(config=yaml.load(
        config), spark=spark_session).run()

    assert dfs[0].rdd.getNumPartitions() == 5


def test_valid_limit_processor(spark_session):
    config = f"""
processors:
  - name: load_employee
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - name: repartition_employee
    type: repartition_processor
    version: v1
    properties:
      repartition:
        num_partitions: 8
        cols:
          - dept_id
  - name: coalesce_employee
    type: repartition_processor
    version: v1
    properties:
      limit: 1
      coalesce:
        num_partitions: 5
relations:
  - left: load_employee
    right: repartition_employee
  - left: repartition_employee
    right: coalesce_employee
"""

    dfs = SparkWorkflowManager(config=yaml.load(
        config), spark=spark_session).run()

    assert dfs[0].count() == 1


def test_valid_persist_processor(spark_session):
    config = f"""
processors:
  - name: load_employee
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - name: persist_employee
    type: persist_processor
    version: v1
    properties:
      storage_level: MEMORY_ONLY



relations:
  - left: load_employee
    right: persist_employee
"""

    dfs = SparkWorkflowManager(config=yaml.load(
        config), spark=spark_session).run()

    assert dfs[0].storageLevel.useMemory is True


def test_valid_show_processor(spark_session, capsys):
    config = f"""
processors:
  - name: load_employee
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - name: show_employee
    type: show_processor
    version: v1
    properties:
      n: 1



relations:
  - left: load_employee
    right: show_employee
"""

    dfs = SparkWorkflowManager(config=yaml.load(
        config), spark=spark_session).run()
    actual, _ = capsys.readouterr()
    dfs[0].show(n=1)
    expected, _ = capsys.readouterr()
    assert actual == expected


def test_valid_union_processor(spark_session):
    config = f"""
processors:
  - name: load_employee1
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - name: load_employee2
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - name: union_employee
    type: union_processor
    version: v1
    properties:
      n: 1



relations:
  - left: load_employee1
    right: union_employee
  - left: load_employee2
    right: union_employee
"""

    dfs = SparkWorkflowManager(config=yaml.load(
        config), spark=spark_session).run()
    df = spark_session.read.option('header', True).csv(
        f'{FIXTURE_DIR}/employee.csv')
    expected = df.union(df).collect()
    actual = dfs[0].collect()
    assert actual == expected


def test_valid_aggregate_processor(spark_session):
    config = f"""
processors:
  - name: load_employee
    type: load_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - name: agg_employee
    type: aggregate_processor
    version: v1
    properties:
      group_by_columns:
        - dept_id
      agg_exprs:
        - max(age) as max_age



relations:
  - left: load_employee
    right: agg_employee
"""

    dfs = SparkWorkflowManager(config=yaml.load(
        config), spark=spark_session).run()
    df = spark_session.read.option('header', True).csv(
        f'{FIXTURE_DIR}/employee.csv')
    agg_exprs = [f.expr(x) for x in ['max(age) as max_age']]
    expected = df.groupBy(['dept_id']).agg(*agg_exprs).collect()
    actual = dfs[0].collect()
    assert actual == expected


def test_missing_implementation(spark_session):
    config = f"""
processors:
  - name: load_employee
    type: random_processor
    version: v1
    properties:
      format: csv
      options:
        header: 'true'
      path: {FIXTURE_DIR}/employee.csv
  - name: repartition_employee
    type: repartition_processor
    version: v1
    properties:
      repartition:
        num_partitions: 8
  - name: coalesce_employee
    type: repartition_processor
    version: v1
    properties:
      coalesce:
        num_partitions: 5



relations:
  - left: load_employee
    right: repartition_employee
  - left: repartition_employee
    right: coalesce_employee
"""

    with pytest.raises(AssertionError) as excinfo:
        SparkWorkflowManager(config=yaml.load(
            config), spark=spark_session).run()
    assert str(
        excinfo.value) == 'SparkProcessor implementation not found ' \
                          'for random_processor with version v1.'
