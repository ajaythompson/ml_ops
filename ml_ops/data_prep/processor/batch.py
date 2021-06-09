from typing import List
from ml_ops.data_prep.processor.property import PropertyDescriptor, \
    PropertyDescriptorBuilder, PropertyGroupDescriptor
from ml_ops.data_prep.processor import ActionProcessor, Dependency
from ml_ops.data_prep.processor import ProcessorContext
from ml_ops.data_prep.processor import TransformProcessor


class LoadProcessor(TransformProcessor):

    LOAD_OPTIONS_GROUP = 'load_options'
    description = 'Processor to read datasource into a dataframe.'

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
            processor_context: ProcessorContext) -> Dependency:
        dependency_config = {}

        default_options = processor_context.get_property_group(
            self.DEFAULT_PROPS_GROUP)
        load_options = processor_context.get_property_group(
            self.LOAD_OPTIONS_GROUP)

        view_name = default_options.get_property(self.VIEW_NAME)
        if view_name is not None:
            dependency_config['view_name'] = view_name

        path = default_options.get_property(self.PATH)
        format = default_options.get_property(self.FORMAT)

        spark_session = processor_context.spark_session
        df = spark_session.read.load(path=path, format=format, **load_options)

        return Dependency(df, dependency_config)


class SQLProcessor(TransformProcessor):

    QUERY = PropertyDescriptorBuilder() \
        .name('query') \
        .description('query to be executed.') \
        .required(True) \
        .build()

    DEFAULT_PROPS_GROUP = PropertyGroupDescriptor(
        group_name='default',
        prop_descriptors=[
            QUERY,
            TransformProcessor.VIEW_NAME
        ]
    )

    def get_property_groups(self) -> List[PropertyGroupDescriptor]:
        return [self.DEFAULT_PROPS_GROUP]

    def run(self,
            processor_context: ProcessorContext) -> Dependency:

        for dependency in processor_context.dependencies:
            assert 'view_name' in dependency.config, \
                'Missing view_name in dependency.'
            view_name = dependency.config['view_name']
            df = dependency.df
            df.createOrReplaceTempView(view_name)

        default_options = processor_context.get_property_group(
            self.DEFAULT_PROPS_GROUP)

        spark = processor_context.spark_session
        df = spark.sql(default_options.get_property(self.QUERY))
        dependency_config = {}
        return Dependency(df, dependency_config)


class WriteProcessor(ActionProcessor):

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
        .allowed_values(['overwrite', 'append']) \
        .default_value('append') \
        .required(False) \
        .build()

    PARTITION_BY = PropertyDescriptorBuilder() \
        .name('partition_by') \
        .description('The columns to partition the df with.') \
        .default_value(None) \
        .required(False) \
        .build()

    DEFAULT_PROPS_GROUP = PropertyGroupDescriptor(
        group_name='default',
        prop_descriptors=[
            PATH,
            FORMAT,
            MODE,
            PARTITION_BY
        ]
    )

    WRITE_OPTIONS_GROUP = PropertyGroupDescriptor(
        group_name='write_options'
    )

    description = 'Processor to write dataframe.'

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

        source_df = processor_context.dependencies[0].df

        source_df.write.save(path=path, format=format,
                             mode=mode, partitionBy=partition_by,
                             **write_options)
