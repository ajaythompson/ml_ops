from ml_ops.data_prep.processor import SparkProcessor
from typing import List

from ml_ops.data_prep.workflow import SparkWorkflowManager
from flask import Flask, session, request, g
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
import uuid
from flask_cors import CORS




def get_workflow():
    return session['workflow']


WORKFLOW_BASE_PATH = '/workflow'


@app.route
def workflow():
    if request.method == 'GET':
        return spar


@app.route(f'{WORKFLOW_BASE_PATH}', methods=['POST'])
def create_workflow():
    if request.method == 'POST':
        return SparkWorkflowManager().create_workflow()


@app.route(f'{PROCESSORS_BASE_PATH}', methods=['GET'])
def get_processors():
    if request.method == 'GET':
        return {'processor_list': list(SparkProcessor._types.keys())}


@app.route(f'{PROCESSOR_BASE_PATH}', methods=['POST'])
def workflow_processor():
    if request.method == 'POST':
        workflow = session['workflow']
        processors: List = workflow['processors']
        data = request.json
        processors.append(data)
        session['workflow'] = workflow
        return get_workflow()


@app.route(f'{PROCESSOR_BASE_PATH}', methods=['GET'])
def processor():
    if request.method == 'GET':
        name = request.args.get('name')
        version = request.args.get('version', 'v1')
        processor: SparkProcessor = SparkProcessor._types[name][version]
        result = {
            'description': processor.description,
            'properties_schema': processor.schema,
        }
        return result


@app.route(f'{RELATION_BASE_PATH}', methods=['POST'])
def relation():
    if request.method == 'POST':
        workflow = session['workflow']
        relations: List = workflow['relations']
        data = request.json
        relations.append(data)
        session['workflow'] = workflow
        return get_workflow()


@app.route(f'{WORKFLOW_RUN_PATH}', methods=['POST'])
def workflow_run():
    spark = get_spark_session()
    workflow = get_workflow()
    workflow_mgr = SparkWorkflowManager.get_workflow_manager(workflow)
    workflow_mgr.run(spark)
    return 'success'


@app.route(f'{WORKFLOW_PREVIEW_PATH}', methods=['POST'])
def preview_workflow():
    spark = get_spark_session()
    workflow_mgr = SparkWorkflowManager.get_workflow_manager(get_workflow())
    result = workflow_mgr.show_json('load_employee', 10, spark)
    result_map = {'result': result}
    return result_map
