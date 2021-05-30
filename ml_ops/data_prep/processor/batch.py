from ml_ops.data_prep.processor.property import PropertyDescriptorBuilder
from ml_ops.data_prep.processor.property import PropertyGroup
from ml_ops.data_prep.processor import ActionProcessor, Dependency
from ml_ops.data_prep.processor import ProcessorContext
from ml_ops.data_prep.processor import TransformProcessor


class LoadProcessor(TransformProcessor):

    LOAD_OPTIONS_GROUP = 'load_options'
    description = 'Processor to read datasource into a dataframe.'

    PATH = PropertyDescriptorBuilder() \
        .name('path') \
        .description('Path or table name to load.') \
        .property_group(TransformProcessor.DEFAULT_PROPS_GROUP) \
        .required(True) \
        .build()

    FORMAT = PropertyDescriptorBuilder() \
        .name('format') \
        .property_group(TransformProcessor.DEFAULT_PROPS_GROUP) \
        .required(True) \
        .build()

    SELECT_COLUMNS = PropertyDescriptorBuilder() \
        .name('format') \
        .description('Comma seperated list of columns') \
        .build()

    def get_property_groups(self):
        property_groups = []

        load_properties = []
        load_properties.append(self.PATH)
        load_properties.append(self.FORMAT)
        load_properties.append(self.SELECT_COLUMNS)
        load_prop_group = PropertyGroup(self.LOAD_PROPERTIES,
                                        load_properties)

        load_options = []
        load_options_group = PropertyGroup(self.LOAD_OPTIONS,
                                           load_options)

        property_groups.append(load_prop_group)
        property_groups.append(load_options_group)
        return property_groups

    def run(self,
            processor_context: ProcessorContext) -> Dependency:
        dependency_config = {}

        view_name = processor_context.get_property(self.VIEW_NAME)
        if view_name is not None:
            dependency_config['view_name'] = view_name

        path = processor_context.get_property(self.PATH)
        format = processor_context.get_property(self.FORMAT)
        options = processor_context.get_property_group(
            LoadProcessor.LOAD_OPTIONS_GROUP)

        if options is None:
            options = {}

        spark_session = processor_context.spark_session
        df = spark_session.read.load(path=path, format=format, **options)

        return Dependency(df, dependency_config)


class SQLProcessor(TransformProcessor):
    DEFAULT_PROPS_GROUP = 'default'

    QUERY = PropertyDescriptorBuilder() \
        .name('query') \
        .description('query to be executed.') \
        .property_group(DEFAULT_PROPS_GROUP) \
        .required(True) \
        .build()

    def get_property_groups(self):
        return []

    def run(self,
            processor_context: ProcessorContext) -> Dependency:

        for dependency in processor_context.dependencies:
            assert 'view_name' in dependency.config, \
                'Missing view_name in dependency.'
            view_name = dependency.config['view_name']
            df = dependency.df
            df.createOrReplaceTempView(view_name)

        spark = processor_context.spark_session
        df = spark.sql(processor_context.get_property(self.QUERY))
        return Dependency(df)


class WriteProcessor(ActionProcessor):

    DEFAULT_PROPS_GROUP = 'default'
    WRITE_OPTIONS_GROUP = 'write_options'
    description = 'Processor to read datasource into a dataframe.'

    PATH = PropertyDescriptorBuilder() \
        .name('path') \
        .description('Path or table name to load.') \
        .property_group(DEFAULT_PROPS_GROUP) \
        .required(True) \
        .build()

    FORMAT = PropertyDescriptorBuilder() \
        .name('format') \
        .property_group(DEFAULT_PROPS_GROUP) \
        .required(True) \
        .build()

    MODE = PropertyDescriptorBuilder() \
        .name('mode') \
        .description('The mode of write.') \
        .allowed_values(['overwrite', 'append']) \
        .default_value('append') \
        .property_group(DEFAULT_PROPS_GROUP) \
        .required(True) \
        .build()

    PARTITION_BY = PropertyDescriptorBuilder() \
        .name('partition_by') \
        .description('The columns to partition the df with.') \
        .default_value(None) \
        .property_group(DEFAULT_PROPS_GROUP) \
        .required(True) \
        .build()

    description = 'Processor to write dataframe.'

    def get_property_groups(self):
        return []

    def run(self,
            processor_context: ProcessorContext) -> None:
        path = processor_context.get_property(self.PATH)
        format = processor_context.get_property(self.FORMAT)
        mode = processor_context.get_property(self.MODE)
        partition_by = processor_context.get_property(self.PARTITION_BY)
        options = processor_context.get_property_group(
            self.WRITE_OPTIONS_GROUP)
        source_df = processor_context.dependencies[0].df

        source_df.write.save(path=path, format=format,
                             mode=mode, partitionBy=partition_by, **options)
