import pickle
from typing import List

from pyspark.sql.types import BinaryType, StringType, StructField, StructType
from sklearn.ensemble import RandomForestClassifier

from ml_ops.processor import ProcessorContext, \
    TransformProcessor, Dependency
from ml_ops.processor.property import PropertyDescriptorBuilder, \
    PropertyGroupDescriptor

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

    DEFAULT_PROPS_GROUP = PropertyGroupDescriptor(
        group_name='default',
        prop_descriptors=[
            N_ESTIMATORS,
            FEATURE_COLS,
            TARGET_COL
        ]
    )

    def get_property_groups(self) -> List[PropertyGroupDescriptor]:
        return [self.DEFAULT_PROPS_GROUP]

    def run(self, processor_context: ProcessorContext) -> Dependency:
        spark = processor_context.spark_session
        dependency = processor_context.dependencies[0]

        default_options = processor_context.get_property_group(
            self.DEFAULT_PROPS_GROUP)
        n_estimators = default_options.get_property(
            self.N_ESTIMATORS,
            self.N_ESTIMATORS.default_value)
        n_estimators = int(n_estimators)
        feature_cols = default_options.get_property(self.FEATURE_COLS).split(
            ',')
        target_col = default_options.get_property(self.TARGET_COL)

        input_df = dependency.df

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
        dep = Dependency(model_df, {})
        return dep


class Inference(TransformProcessor):
    MODEL_LOCATION = PropertyDescriptorBuilder() \
        .name('model_location') \
        .description('Parquet file where model is available.') \
        .required(True) \
        .build()

    FEATURE_COLS = PropertyDescriptorBuilder() \
        .name('feature_cols') \
        .description('Comma separated feature columns.') \
        .required(True) \
        .build()

    DEFAULT_PROPS_GROUP = PropertyGroupDescriptor(
        group_name='default',
        prop_descriptors=[
            MODEL_LOCATION,
            FEATURE_COLS
        ]
    )

    def get_property_groups(self) -> List[PropertyGroupDescriptor]:
        return [self.DEFAULT_PROPS_GROUP]

    def run(self, processor_context: ProcessorContext) -> Dependency:
        spark = processor_context.spark_session
        dependency = processor_context.dependencies[0]

        default_options = processor_context.get_property_group(
            self.DEFAULT_PROPS_GROUP)
        feature_cols = default_options.get_property(self.FEATURE_COLS).split(
            ',')
        model_location = default_options.get_property(self.MODEL_LOCATION)

        model_df = spark.read.parquet(model_location).select(MODEL_COLUMN_NAME)
        model_row = model_df.collect()[0]
        model_binary = model_row[0]
        model = pickle.loads(model_binary)

        feature_df = dependency.df
        feature_pdf = feature_df.toPandas()

        output = model.predict(feature_pdf[feature_cols])
        output_list = [[x] for x in output]

        output_df = spark.createDataFrame(output_list)

        dep = Dependency(output_df, {})
        return dep
