import json

from pyspark.sql.types import StructType

from ml_ops.processor import ActionProcessor, Dependency, \
    ProcessorContext, TransformProcessor
from ml_ops.processor.configuration_constants import TRIGGER_TYPES,\
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
            processor_context: ProcessorContext) -> Dependency:

        logger = processor_context.get_logger()

        dependency_config = {}

        default_options = processor_context.get_property_group(
            self.DEFAULT_PROPS_GROUP)
        load_options = processor_context.get_property_group(
            self.LOAD_OPTIONS_GROUP)

        view_name = default_options.get_property(self.VIEW_NAME)
        if view_name is not None:
            dependency_config['view_name'] = view_name

        path = default_options.get_property(self.PATH)
        load_format = default_options.get_property(self.FORMAT)
        schema = default_options.get_property(self.SCHEMA)

        json_schema = json.loads(schema)
        logger.info(f'Reading data using schema {json_schema}.')
        struct_type = StructType.fromJson(json_schema)

        df = processor_context.spark_session.readStream.load(
            path=path, format=load_format, schema=struct_type, **load_options)

        return Dependency(df, dependency_config)


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

    DEFAULT_PROPS_GROUP = PropertyGroupDescriptor(
        group_name='default',
        prop_descriptors=[
            PATH,
            FORMAT,
            MODE,
            PARTITION_BY,
            TRIGGER_TYPE,
            TRIGGER_VALUE
        ]
    )

    WRITE_OPTIONS_GROUP = PropertyGroupDescriptor(
        group_name='write_options'
    )

    def get_property_groups(self):
        return [self.DEFAULT_PROPS_GROUP,
                self.WRITE_OPTIONS_GROUP]

    def run(self,
            processor_context: ProcessorContext) -> None:
        default_options = processor_context.get_property_group(
            self.DEFAULT_PROPS_GROUP
        )

        write_options = processor_context.get_property_group(
            self.WRITE_OPTIONS_GROUP
        )

        path = default_options.get_property(self.PATH)
        write_format = default_options.get_property(self.FORMAT)
        mode = default_options.get_property(self.MODE)
        partition_by = default_options.get_property(self.PARTITION_BY)
        trigger_type = default_options.get_property(
            self.TRIGGER_TYPE, self.TRIGGER_TYPE.default_value)
        trigger_value = default_options.get_property(
            self.TRIGGER_VALUE, self.TRIGGER_VALUE.default_value)
        if trigger_type == ONCE_TRIGGER_TYPE:
            trigger_value = get_boolean_value(trigger_value, True)

        source_df = processor_context.dependencies[0].df

        trigger_params = {trigger_type: trigger_value}

        # TODO check query name

        source_df \
            .writeStream \
            .trigger(**trigger_params) \
            .start(path=path,
                   format=write_format,
                   outputMode=mode,
                   partitionBy=partition_by,
                   **write_options) \
            .awaitTermination()
