from typing import List

from ml_ops.processor import ActionProcessor, FlowDF, RelationDescriptor
from ml_ops.processor import ProcessorContext
from ml_ops.processor import PropertyDescriptorBuilder
from ml_ops.processor import TransformProcessor


class LoadProcessor(TransformProcessor):
    PATH = PropertyDescriptorBuilder() \
        .name('path') \
        .description('Path or table name to load.') \
        .required(True) \
        .build()

    FORMAT = PropertyDescriptorBuilder() \
        .name('format') \
        .required(True) \
        .build()

    def get_property_descriptors(self):
        return [
            self.PATH,
            self.FORMAT
        ]

    def get_relations(self):
        return []

    def run(self,
            processor_context: ProcessorContext) -> FlowDF:
        dependency_config = {}

        view_name = processor_context.get_property(self.VIEW_NAME)
        if view_name is not None:
            dependency_config['view_name'] = view_name

        path = processor_context.get_property(self.PATH)
        load_format = processor_context.get_property(self.FORMAT)

        load_options = {}

        for key, value in processor_context.get_properties().items():
            if key.startswith('read.'):
                key = key.rsplit(sep='.', maxsplit=1)[1]

                load_options[key] = value

        spark_session = processor_context.spark_session
        df = spark_session.read.load(path=path,
                                     format=load_format,
                                     **load_options)
        return FlowDF(df, dependency_config)


class SQLProcessor(TransformProcessor):
    QUERY = PropertyDescriptorBuilder() \
        .name('query') \
        .description('query to be executed.') \
        .required(True) \
        .build()

    def get_property_descriptors(self):
        return [
            self.QUERY
        ]

    def get_relations(self):
        return []

    def run(self,
            processor_context: ProcessorContext) -> FlowDF:

        for dependency in processor_context.get_flow_dfs():
            assert 'view_name' in dependency.attributes, \
                'Missing view_name in dependency.'
            view_name = dependency.attributes['view_name']
            df = dependency.df
            df.createOrReplaceTempView(view_name)

        spark = processor_context.spark_session
        query = processor_context.get_property(self.QUERY)
        processor_context.get_logger().info(f'Executing query {query}.')
        df = spark.sql(query)
        dependency_config = {}
        view_name = processor_context.get_property(self.VIEW_NAME)
        if view_name is not None:
            dependency_config['view_name'] = view_name
        return FlowDF(df, dependency_config)


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

    INPUT_RELATION = RelationDescriptor(name='INPUT')

    def get_property_descriptors(self):
        return [
            self.PATH,
            self.FORMAT,
            self.MODE,
            self.PARTITION_BY,
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

        write_options = {}

        for key, value in processor_context.get_properties().items():
            if key.startswith('write.'):
                key = key.rsplit(sep='.', maxsplit=1)[1]

                write_options[key] = value

        input_df = processor_context.get_flow_df(self.INPUT_RELATION).df

        input_df.write.save(path=path, format=write_format,
                            mode=mode, partitionBy=partition_by,
                            **write_options)
