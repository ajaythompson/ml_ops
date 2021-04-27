from ml_ops.data_prep.workflow.processors.spark.processor import SparkProcessor
from typing import List

from ml_ops.data_prep.workflow import SparkWorkflowManager
from flask import Flask, session, request, g
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
import uuid
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
# app.config.from_envvar('MLOPS_CONFIG_PATH')
app.config

# Set the secret key to some random bytes. Keep this really secret!
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'


def get_global_spark_session():
    if 'spark' not in g:
        conf = SparkConf()
        conf.set("spark.master", "local[*]")
        conf.set("spark.app.name", "tests_helper")
        conf.set("spark.sql.shuffle.partitions", "8")
        g.sc = SparkSession.builder.config(conf=conf).getOrCreate()
    return g.sc


def get_session_id():
    if '_ml_id' not in session:
        session['_ml_id'] = str(uuid.uuid4())


def get_spark_session():
    session_id = get_session_id()

    if 'ss' not in g:
        g.sessions = {}

    if session_id not in g.sessions:
        g.sessions[session_id] = {}

    if 'spark_session' not in g.sessions[session_id]:
        g.sessions[session_id]['spark_session'] = get_global_spark_session() \
            .newSession()

    return g.sessions[session_id]['spark_session']


def get_workflow():
    return session['workflow']


def create_workflow():
    session['workflow'] = SparkWorkflowManager().get_config()
    return session['workflow']


WORKFLOW_BASE_PATH = '/workflow'
PROCESSORS_BASE_PATH = f'{WORKFLOW_BASE_PATH}/processors'
PROCESSOR_BASE_PATH = f'{WORKFLOW_BASE_PATH}/processor'
RELATION_BASE_PATH = f'{WORKFLOW_BASE_PATH}/relation'
WORKFLOW_RUN_PATH = f'{PROCESSOR_BASE_PATH}/run'
WORKFLOW_PREVIEW_PATH = f'{PROCESSOR_BASE_PATH}/preview'


@app.route(f'{WORKFLOW_BASE_PATH}', methods=['GET', 'POST'])
def workflow():
    if request.method == 'POST':
        return create_workflow()
    else:
        return get_workflow()


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
