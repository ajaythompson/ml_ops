import json
from typing import List

from pyspark.sql.types import StructType

from ml_ops.processor import ActionProcessor, FlowDF, \
    ProcessorContext, TransformProcessor, RelationDescriptor
from ml_ops.processor.configuration_constants import TRIGGER_TYPES, \
    ONCE_TRIGGER_TYPE
from ml_ops.processor.property import PropertyDescriptorBuilder, \
    PropertyGroupDescriptor, get_boolean_value


class LoadStreamProcessor(TransformProcessor):
    PATH = PropertyDescriptorBuilder() \
        .name('path') \
        .description('Path or table name to load.') \
        .required(True) \
        .build()

    FORMAT = PropertyDescriptorBuilder() \
        .name('format') \
        .required(True) \
        .build()

    SCHEMA = PropertyDescriptorBuilder() \
        .name('schema') \
        .required(True) \
        .build()

    def get_property_descriptors(self):
        return [
            self.PATH,
            self.FORMAT,
            self.SCHEMA,
        ]

    def get_relations(self):
        return []

    DEFAULT_PROPS_GROUP = PropertyGroupDescriptor(
        group_name='default',
        prop_descriptors=[
            PATH,
            FORMAT,
            SCHEMA,
            TransformProcessor.VIEW_NAME
        ]
    )

    LOAD_OPTIONS_GROUP = PropertyGroupDescriptor(
        group_name='load_options'
    )

    def get_property_groups(self):
        return [self.DEFAULT_PROPS_GROUP,
                self.LOAD_OPTIONS_GROUP]

    def run(self,
            processor_context: ProcessorContext) -> FlowDF:

        logger = processor_context.get_logger()

        dependency_config = {}

        load_options = {}

        for key, value in processor_context.get_properties().items():
            if key.startswith('read.'):
                key = key.rsplit(sep='.', maxsplit=1)[1]

                load_options[key] = value

        view_name = processor_context.get_property(self.VIEW_NAME)
        if view_name is not None:
            dependency_config['view_name'] = view_name

        path = processor_context.get_property(self.PATH)
        load_format = processor_context.get_property(self.FORMAT)
        schema = processor_context.get_property(self.SCHEMA)

        json_schema = json.loads(schema)
        logger.info(f'Reading data using schema {json_schema}.')
        struct_type = StructType.fromJson(json_schema)

        df = processor_context.spark_session.readStream.load(
            path=path, format=load_format, schema=struct_type, **load_options)

        return FlowDF(df, dependency_config)


class WriteStreamProcessor(ActionProcessor):
    PATH = PropertyDescriptorBuilder() \
        .name('path') \
        .description('Path or table name to load.') \
        .required(True) \
        .build()

    FORMAT = PropertyDescriptorBuilder() \
        .name('format') \
        .required(True) \
        .build()

    MODE = PropertyDescriptorBuilder() \
        .name('mode') \
        .description('The mode of write.') \
        .allowed_values(['append', 'complete', 'update']) \
        .default_value('append') \
        .required(False) \
        .build()

    PARTITION_BY = PropertyDescriptorBuilder() \
        .name('partition_by') \
        .description('The columns to partition the df with.') \
        .default_value(None) \
        .required(False) \
        .build()

    TRIGGER_TYPE = PropertyDescriptorBuilder() \
        .name('trigger_type') \
        .description('The type of trigger.') \
        .required(False) \
        .allowed_values(TRIGGER_TYPES) \
        .default_value(ONCE_TRIGGER_TYPE) \
        .build()

    TRIGGER_VALUE = PropertyDescriptorBuilder() \
        .name('trigger_value') \
        .description('The value that has to be set to the trigger.') \
        .required(False) \
        .default_value('true') \
        .build()

    INPUT_RELATION = RelationDescriptor(name='INPUT')

    def get_property_descriptors(self):
        return [
            self.PATH,
            self.FORMAT,
            self.MODE,
            self.PARTITION_BY,
            self.TRIGGER_TYPE,
            self.TRIGGER_VALUE,
        ]

    def get_relations(self) -> List[RelationDescriptor]:
        return [
            self.INPUT_RELATION
        ]

    def run(self,
            processor_context: ProcessorContext) -> None:
        path = processor_context.get_property(self.PATH)
        write_format = processor_context.get_property(self.FORMAT)
        mode = processor_context.get_property(self.MODE)
        partition_by = processor_context.get_property(self.PARTITION_BY)
        trigger_type = processor_context.get_property(
            self.TRIGGER_TYPE, self.TRIGGER_TYPE.default_value)
        trigger_value = processor_context.get_property(
            self.TRIGGER_VALUE, self.TRIGGER_VALUE.default_value)
        if trigger_type == ONCE_TRIGGER_TYPE:
            trigger_value = get_boolean_value(trigger_value, True)

        source_df = processor_context.get_flow_df(self.INPUT_RELATION).df

        trigger_params = {trigger_type: trigger_value}

        # TODO check query name

        write_options = {}

        for key, value in processor_context.get_properties().items():
            if key.startswith('write.'):
                key = key.rsplit(sep='.', maxsplit=1)[1]

                write_options[key] = value

        source_df \
            .writeStream \
            .trigger(**trigger_params) \
            .start(path=path,
                   format=write_format,
                   outputMode=mode,
                   partitionBy=partition_by,
                   **write_options) \
            .awaitTermination()
