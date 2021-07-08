import os
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ml_ops.processor import ProcessorContext, PropertyGroups, PropertyGroup, \
    Dependency
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
    trainer_property_group = PropertyGroup()
    features_cols = ['sepal.length',
                     'sepal.width',
                     'petal.length',
                     'petal.width']
    trainer_property_group.set_property(Trainer.FEATURE_COLS,
                                        ','.join(features_cols))
    trainer_property_group.set_property(Trainer.TARGET_COL, 'variety')

    trainer_property_groups = PropertyGroups()
    trainer_property_groups.set_property_group(
        Trainer.DEFAULT_PROPS_GROUP,
        trainer_property_group)

    csv_options = {
        'inferSchema': 'true',
        'header': 'true'
    }

    iris_data_path = f'{FIXTURE_DIR}/iris.csv'
    input_df = spark.read.format('csv').options(**csv_options) \
        .load(iris_data_path)
    input_dependency = Dependency(df=input_df, config={})

    trainer_processor_context = ProcessorContext(
        spark_session=spark,
        property_groups=trainer_property_groups,
        dependencies=[input_dependency])
    random_forest_trainer = Trainer()
    trainer_dep = random_forest_trainer.run(trainer_processor_context)

    model_location = f'{temp_dir}/output'
    trainer_dep.df.write.mode('overwrite').parquet(model_location)

    inference_property_group = PropertyGroup()
    features_cols = ['sepal.length',
                     'sepal.width',
                     'petal.length',
                     'petal.width']
    inference_property_group.set_property(Inference.FEATURE_COLS,
                                          ','.join(features_cols))
    inference_property_group.set_property(Inference.MODEL_LOCATION,
                                          model_location)

    inference_property_groups = PropertyGroups()
    inference_property_groups.set_property_group(
        Inference.DEFAULT_PROPS_GROUP,
        inference_property_group)

    inference_processor_context = ProcessorContext(
        spark_session=spark,
        property_groups=inference_property_groups,
        dependencies=[input_dependency])

    random_forest_inference = Inference()
    inference_dep = random_forest_inference.run(inference_processor_context)
    inference_dep.df.show()
