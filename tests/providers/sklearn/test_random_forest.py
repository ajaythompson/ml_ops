import os
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ml_ops.processor import ProcessorContext, FlowDF
from ml_ops.providers.sklearn.random_forest import Inference, Trainer

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'test_files',
)


@pytest.fixture
def temp_dir():
    temp_dir = f'{FIXTURE_DIR}/temp'
    Path(temp_dir).mkdir(exist_ok=True)
    yield temp_dir
    shutil.rmtree(path=temp_dir, ignore_errors=True)


def test_random_forest_trainer(spark: SparkSession,
                               temp_dir: str):
    features_cols = ['sepal.length',
                     'sepal.width',
                     'petal.length',
                     'petal.width']

    trainer_processor_context = ProcessorContext(spark)
    trainer_processor_context.set_property(Trainer.FEATURE_COLS,
                                           ','.join(features_cols))
    trainer_processor_context.set_property(Trainer.TARGET_COL, 'variety')

    csv_options = {
        'inferSchema': 'true',
        'header': 'true'
    }

    iris_data_path = f'{FIXTURE_DIR}/iris.csv'
    input_df = spark.read.format('csv').options(**csv_options) \
        .load(iris_data_path)
    input_flow_df = FlowDF(df=input_df, attributes={})

    trainer_processor_context.set_flow_df(Trainer.INPUT_RELATION,
                                          input_flow_df)

    random_forest_trainer = Trainer()
    trainer_dep = random_forest_trainer.run(trainer_processor_context)

    model_flow_df = FlowDF(trainer_dep.df, {})

    inference_processor_context = ProcessorContext(spark)
    inference_processor_context.set_property(Inference.FEATURE_COLS,
                                             ','.join(features_cols))

    inference_processor_context.set_flow_df(Inference.INPUT_RELATION,
                                            input_flow_df)
    inference_processor_context.set_flow_df(Inference.MODEL_RELATION,
                                            model_flow_df)

    random_forest_inference = Inference()
    inference_dep = random_forest_inference.run(inference_processor_context)
    inference_dep.df.show()
