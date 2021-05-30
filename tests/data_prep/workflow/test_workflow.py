from ml_ops.data_prep.workflow import InMemoryWFRepository, \
    WFProcessor, WFRelation
from ml_ops.data_prep.workflow import SparkWorkflowManager
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
import pytest
import os
import shutil

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


def get_load_processor(name,
                       path,
                       format,
                       options={},
                       view_name=None) -> WFProcessor:
    property_groups = {
        'default': {
            'path': path,
            'format': format
        },
        'load_options': options
    }

    if view_name is not None:
        property_groups['default']['view_name'] = view_name

    load_processor = WFProcessor(name=name,
                                 type='LoadProcessor',
                                 property_groups=property_groups
                                 )
    return load_processor


def get_sql_processor(name, query) -> WFProcessor:
    property_groups = {
        'default': {
            'query': query
        },
    }

    sql_processor = WFProcessor(name=name,
                                type='SQLProcessor',
                                property_groups=property_groups
                                )

    return sql_processor


def get_write_processor(name,
                        path,
                        format,
                        options={}) -> WFProcessor:
    property_groups = {
        'default': {
            'path': path,
            'format': format
        },
        'write_options': options
    }

    write_processor = WFProcessor(name=name,
                                  type='WriteProcessor',
                                  property_groups=property_groups
                                  )
    return write_processor


def test_workflow_show_json(spark: SparkSession):
    wf_repo = InMemoryWFRepository()
    wf_manager = SparkWorkflowManager(wf_repo)
    workflow = wf_manager.create_workflow()

    load_options = {
        'header': 'true'
    }
    load_processor = get_load_processor(
        name='load_employee',
        path=f'{FIXTURE_DIR}/employee.csv',
        format='csv',
        options=load_options)

    workflow.add_processor(load_processor)

    actual = wf_manager.show_json(workflow.id, load_processor.id, spark)
    expected = [
        {'age': '30', 'dept_id': '1', 'emp_id': '1', 'name': 'e1'},
        {'age': '33', 'dept_id': '2', 'emp_id': '2', 'name': 'e2'}]
    assert actual == expected


def test_workflow_multi_processor(spark: SparkSession):
    wf_repo = InMemoryWFRepository()
    wf_manager = SparkWorkflowManager(wf_repo)
    workflow = wf_manager.create_workflow()

    csv_options = {
        'header': 'true'
    }

    load_employee = get_load_processor(
        name='load_employee',
        path=f'{FIXTURE_DIR}/employee.csv',
        format='csv',
        options=csv_options,
        view_name='employee')

    load_department = get_load_processor(
        name='load_department',
        path=f'{FIXTURE_DIR}/department.csv',
        format='csv',
        options=csv_options,
        view_name='department')

    join_employee_department = get_sql_processor(
        name='join_emp_dept',
        query="""
        SELECT emp_id, name, department.dept
        FROM employee
        JOIN department  ON employee.dept_id = department.dept_id
        WHERE emp_id='1'
        """
    )

    write_enriched_data = get_write_processor(
        name='write_enriched_data',
        path=f'{TEST_DIR}/enriched_employee',
        format='csv',
        options=csv_options
    )

    processors = [load_employee,
                  load_department,
                  join_employee_department,
                  write_enriched_data]

    for processor in processors:
        workflow.add_processor(processor)

    relations = [
        (load_employee.id, join_employee_department.id),
        (load_department.id, join_employee_department.id),
        (join_employee_department.id, write_enriched_data.id)
    ]

    for left, right in relations:
        workflow.add_relation(
            WFRelation(left, right)
        )

    wf_manager.run(workflow_id=workflow.id,
                   spark=spark,
                   processor_id=write_enriched_data.id)

    actual = spark.read.format('csv').options(**csv_options) \
        .load(f'{TEST_DIR}/enriched_employee') \
        .collect()

    expected = spark.read.format('csv').options(**csv_options) \
        .load(f'{FIXTURE_DIR}/enriched_employee_output.csv') \
        .collect()

    assert actual == expected
