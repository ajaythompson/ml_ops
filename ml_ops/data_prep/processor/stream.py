from ml_ops.data_prep.processor.property import PropertyDescriptorBuilder, \
    PropertyGroupDescriptor
from pyspark.sql.dataframe import DataFrame
from ml_ops.data_prep.processor import ActionProcessor, ProcessorContext, \
    TransformProcessor


class LoadStreamProcessor(TransformProcessor):

    LOAD_OPTIONS_GROUP = 'load_options'

    PATH = PropertyDescriptorBuilder() \
        .name('path') \
        .description('Path or table name to load.') \
        .required(True) \
        .build()

    FORMAT = PropertyDescriptorBuilder() \
        .name('format') \
        .required(True) \
        .build()

    DEFAULT_PROPS_GROUP = PropertyGroupDescriptor(
        group_name='default',
        prop_descriptors=[
            PATH,
            FORMAT,
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
            processor_context: ProcessorContext) -> DataFrame:

        default_options = processor_context.get_property_group(
            self.DEFAULT_PROPS_GROUP)
        load_options = processor_context.get_property_group(
            self.LOAD_OPTIONS_GROUP)

        path = default_options.get(self.PATH)
        format = default_options.get(self.FORMAT)

        df = processor_context.spark_session.readStream.load(
            path=path,
            format=format,
            **load_options)
        return df


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
        .allowed_values(['overwrite', 'append', 'update']) \
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
        .allowed_values(['processingTime', 'once', 'continuous']) \
        .default_value('processingTime')

    TRIGGER_VALUE = PropertyDescriptorBuilder() \
        .name('trigger_value') \
        .description('The value that has to be set to the trigger.')

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
        format = default_options.get_property(self.FORMAT)
        mode = default_options.get_property(self.MODE)
        partition_by = default_options.get_property(self.PARTITION_BY)
        trigger_type = default_options.get_property(self.TRIGGER_TYPE)
        trigger_value = default_options.get_property(self.TRIGGER_VALUE)

        source_df = processor_context.dependencies[0].df

        trigger_params = {trigger_type: trigger_value}

        # TODO check query name

        source_df \
            .writeStream \
            .trigger(**trigger_params) \
            .start(path=path,
                   format=format,
                   outputMode=mode,
                   partitionBy=partition_by
                   ** write_options)
