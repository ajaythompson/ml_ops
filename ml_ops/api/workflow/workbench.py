from typing import Dict
import uuid

from flask import g, Flask, request, render_template, send_from_directory
from flask_cors import CORS
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from ml_ops.processor import SparkProcessor
from ml_ops.workflow import WorkflowRepository, ProcessorConfig, \
    SparkWorkflowManager, ConnectionConfig


UI_BUILD_PATH = '../../ui/build'
UI_STATIC_PATH = f'{UI_BUILD_PATH}/static'

app = Flask(__name__,
            static_folder=UI_STATIC_PATH,
            template_folder=UI_BUILD_PATH)

cors = CORS(app, resources={r"/*": {"origins": "*"}})

WORKFLOW_BASE_PATH = '/workflow'
PROCESSORS_BASE_PATH = '/processors'
PROCESSOR_BASE_PATH = '/processor'


def get_workflow_repo(repo_type: str = 'InMemoryWFRepository') -> \
        WorkflowRepository:
    if 'repository' not in g:
        g.repository = WorkflowRepository.get_repository(repo_type)

    return g.repository


def get_spark_session(conf: dict) -> SparkSession:
    if conf is None:
        conf = {}

    if 'spark_session' not in g:
        spark_conf = SparkConf()
        spark_conf.setAll(conf.items())
        g.spark_session = SparkSession \
            .builder \
            .config(conf=spark_conf) \
            .getOrCreate()

    return g.spark_session


def get_workflow_manager(wf_repo: WorkflowRepository) -> SparkWorkflowManager:
    if 'workflow_manager' not in g:
        g.workflow_manager = SparkWorkflowManager(wf_repo)
    return g.workflow_manager


with app.app_context():
    repo = get_workflow_repo()
    spark = get_spark_session({})
    workflow_manager = get_workflow_manager(repo)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/manifest.json')
def manifest():
    return send_from_directory(UI_BUILD_PATH, 'manifest.json')


@app.route('/logo192.png')
def logo():
    return send_from_directory(UI_BUILD_PATH, 'logo192.png')


@app.route('/favicon.ico')
def icon():
    return send_from_directory(UI_BUILD_PATH, 'favicon.ico')


@app.route(f'{WORKFLOW_BASE_PATH}/<workflow_id>', methods=['GET'])
def get_workflow(workflow_id):
    workflow = repo.read_workflow(workflow_id)
    if workflow is None:
        raise Exception(f'Workflow with id {workflow_id} not found!')
    return workflow.json_value()


@app.route(f'{WORKFLOW_BASE_PATH}', methods=['POST', 'GET'])
def create_workflow():

    if request.method == 'POST':
        return repo.create_workflow().json_value()
    else:
        result = {
            'workflows': [x.json_value() for x in repo.get_workflows()]
        }
        return result


@app.route(f'{WORKFLOW_BASE_PATH}/<workflow_id>/processor',
           methods=['POST', 'PUT'])
def add_processor(workflow_id):
    workflow = repo.read_workflow(workflow_id)
    if workflow is None:
        raise Exception('Workflow not found.')
    processor_id = request.json.get('id')
    processor_config = ProcessorConfig.get_processor(request.json)

    if request.method == 'PUT':
        if processor_id is None:
            processor_id = str(uuid.uuid1())
            processor_config.processor_id = processor_id
        return workflow.update_processor(processor_id,
                                         processor_config).json_value()
    elif request.method == 'DELETE':
        return workflow.remove_processor(processor_id)


@app.route(f'{WORKFLOW_BASE_PATH}/<workflow_id>/run/<processor_id>')
def run_processor(workflow_id, processor_id):
    result = workflow_manager.show_json(workflow_id, processor_id, spark)
    return {'result': result}


@app.route(f'{WORKFLOW_BASE_PATH}/<workflow_id>/run', methods=['POST'])
def run_workflow(workflow_id):
    workflow_manager.run(workflow_id, spark)

    return {'status': 'OK'}


@app.route(f'{WORKFLOW_BASE_PATH}/<workflow_id>/connection',
           methods=['POST'])
def add_connection(workflow_id):
    workflow = repo.read_workflow(workflow_id)
    connection_config = ConnectionConfig.get_relation(request.json)
    return workflow.add_relation(connection_config).json_value()


@app.route(f'{PROCESSORS_BASE_PATH}', methods=['GET'])
def list_processors() -> Dict[str, list]:
    processor_types = SparkProcessor.types
    return {'processors': list(processor_types.keys())}


@app.route(f'{PROCESSOR_BASE_PATH}/<processor_name>', methods=['GET'])
def get_processor(processor_name: str):
    processor = SparkProcessor.get_spark_processor(processor_name)
    return processor.to_json()
