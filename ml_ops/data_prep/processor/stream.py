from pyspark.sql.dataframe import DataFrame
from ml_ops.data_prep.workflow.processors.spark.processor import SparkProcessor


class LoadStreamProcessor(
        SparkProcessor, type='load_stream_processor', version='v1'):

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
                        'format': {'type': 'string'},
                        'options': {'type': 'object'},
                        'select_columns': {
                            'type': 'array',
                            'items': {
                                'type': 'string'
                            }
                        },
                        'filter_condition': {'type': 'string'},
                        'limit': {'type': 'number'}
                    },
                    'required': ['format']
                }
        }
    }

    @SparkProcessor.select
    @SparkProcessor.limit
    @SparkProcessor.filter
    def run(self) -> DataFrame:
        options = self.properties.get('options', {})

        load_params = {
            'path': self.properties.get('path', None),
            'format': self.properties.get['format'],
        }

        df = self.spark.readStream.load(**load_params, **options)
        return df


class WriteStreamProcessor(
        SparkProcessor, type='write_stream_processor', version='v1'):

    schema = {
        'type': 'object',
        'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'properties': {
                    'type': 'object',
                    'properties': {
                        'path': {'type': 'string'},
                        'format': {'type': 'string'},
                        'output_mode': {
                            'type': 'string',
                                    'enum': ['append', 'complete', 'update']
                        },
                        'partition_by': {
                            'type': 'array',
                                    'items': {'type': 'string'}
                        },
                        'options': {'type': 'object'},
                        'trigger': {
                            'type': 'object',
                            'properties': {
                                'oneOf': [
                                    {
                                        'processing_time': {'type': 'string'}
                                    },
                                    {
                                        'once': {'type': 'boolean'}
                                    },
                                    {
                                        'continuous': {'type': 'string'}
                                    }
                                ]
                            }
                        }

                    },
                    'required': ['format', 'output_mode']
                }
        }
    }
