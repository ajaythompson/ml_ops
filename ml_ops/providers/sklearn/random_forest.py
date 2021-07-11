import pickle

from pyspark.sql.types import BinaryType, StringType, StructField, StructType
from sklearn.ensemble import RandomForestClassifier

from ml_ops.processor import ProcessorContext, \
    TransformProcessor, FlowDF, RelationDescriptor
from ml_ops.processor.property import PropertyDescriptorBuilder

MODEL_COLUMN_NAME = 'model'


class Trainer(TransformProcessor):
    N_ESTIMATORS = PropertyDescriptorBuilder() \
        .name('n_estimators') \
        .description('number of estimators.') \
        .required(False) \
        .default_value('100') \
        .build()

    FEATURE_COLS = PropertyDescriptorBuilder() \
        .name('feature_cols') \
        .description('Comma separated feature columns.') \
        .required(True) \
        .build()

    TARGET_COL = PropertyDescriptorBuilder() \
        .name('target_col') \
        .description('number of estimators.') \
        .required(True) \
        .build()

    INPUT_RELATION = RelationDescriptor(name='INPUT')

    def get_property_descriptors(self):
        return [
            self.N_ESTIMATORS,
            self.FEATURE_COLS,
            self.TARGET_COL,
        ]

    def get_relations(self):
        return [self.INPUT_RELATION]

    def run(self, processor_context: ProcessorContext) -> FlowDF:
        spark = processor_context.spark_session
        input = processor_context.get_flow_df(self.INPUT_RELATION)

        n_estimators = processor_context.get_property(
            self.N_ESTIMATORS,
            self.N_ESTIMATORS.default_value)
        n_estimators = int(n_estimators)
        feature_cols = processor_context.get_property(self.FEATURE_COLS).split(
            ',')
        target_col = processor_context.get_property(self.TARGET_COL)

        input_df = input.df

        input_pdf = input_df.toPandas()

        x = input_pdf[feature_cols]
        y = input_pdf[target_col]

        clf = RandomForestClassifier(n_estimators=n_estimators)
        clf.fit(X=x, y=y)

        data = [['random_forest_classifier', bytearray(pickle.dumps(clf))]]
        fields = [StructField(name='model_name', dataType=StringType()),
                  StructField(name='model', dataType=BinaryType())]
        data_schema = StructType(fields=fields)

        model_df = spark.createDataFrame(data, schema=data_schema)
        model_df.show()
        dep = FlowDF(model_df, {})
        return dep


class Inference(TransformProcessor):

    FEATURE_COLS = PropertyDescriptorBuilder() \
        .name('feature_cols') \
        .description('Comma separated feature columns.') \
        .required(True) \
        .build()

    INPUT_RELATION = RelationDescriptor(name='INPUT')
    MODEL_RELATION = RelationDescriptor(name='MODEL')

    def get_property_descriptors(self):
        return [
            self.FEATURE_COLS,
        ]

    def get_relations(self):
        return [
            self.INPUT_RELATION,
            self.MODEL_RELATION,
        ]

    def run(self, processor_context: ProcessorContext) -> FlowDF:
        spark = processor_context.spark_session
        input_rel = processor_context.get_flow_df(self.INPUT_RELATION)
        model = processor_context.get_flow_df(self.MODEL_RELATION)

        feature_cols = processor_context.get_property(self.FEATURE_COLS).split(
            ',')

        model_df = model.df.select('model')
        model_row = model_df.collect()[0]
        model_binary = model_row[0]
        model = pickle.loads(model_binary)

        feature_df = input_rel.df
        feature_pdf = feature_df.toPandas()

        output = model.predict(feature_pdf[feature_cols])
        output_list = [[x] for x in output]

        output_df = spark.createDataFrame(output_list)

        dep = FlowDF(output_df, {})
        return dep
