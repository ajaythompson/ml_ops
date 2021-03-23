from ml_ops.data_prep.composer import SparkDataPrepComposer
import os
import pytest
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'test_files',
)

@pytest.fixture
def spark_session():
    spark_options = {
        'spark.app.name': 'test_data_prep',
        'spark.master': 'local[1]',
    }
    yield SparkSession.builder.config(
            conf=SparkConf(list(spark_options))).getOrCreate()



def test_compose(spark_session):
    edges = [('load', 'write')]
    config = {
        'load': {
            'type': 'load',
            'config': {
                'path': f'{FIXTURE_DIR}/load.csv',
                'format': 'csv',
                'options': {
                    'header': 'true'
                }
            }
        },
        'write': {
            'type': 'write',
            'config': {
                'path': f'{FIXTURE_DIR}/output/write.csv',
                'format': 'csv',
                'options': {
                    'header': 'true'
                }
            }
        },
    }
    spark_options = {
        'spark.app.name': 'test_data_prep',
        'spark.master': 'local[1]',
    }
    SparkDataPrepComposer(edges=edges, config=config, spark_session=spark_session)()
    
