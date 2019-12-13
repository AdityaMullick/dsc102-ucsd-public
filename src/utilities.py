from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os
from math import isclose
SEED = 102
TASK_NAMES = ['task_' + str(i) for i in range(1, 7)]
EXT = '.json'


class PA2Test(object):
    def __init__(self, spark, test_results_root):
        self.spark = spark
        self.test_results_root = test_results_root
        self.dict_res = {}
        for task_name in TASK_NAMES:
            try:
                df = self.spark.read.json(
                    os.path.join(test_results_root, task_name + EXT),
                )
                self.dict_res[task_name] = df.collect()[0].asDict()
            except Exception:
                print (Exception)

    def test(self, res, task_name):
        failures = []
        count = 0
        ref_res = self.dict_res[task_name]
        for k, v in ref_res.items():
            count += 1
            print ('Test {}/{} : {} ... '.format(count, len(ref_res), k), end='')  # noqa
            try:
                assert isclose(
                    res[k], v, abs_tol=10**-1), \
                    'Value of {} should be {}, but got {} instead'.format(
                        k, v, res[k])
                print('Pass')
            except Exception as e:
                failures.append(e)
                print('Fail: {}'.format(e))
        print('{}/{} passed'.format(len(ref_res) - len(failures), len(ref_res)))


def spark_init(pid):
    spark = SparkSession.builder.appName(pid).getOrCreate()
    spark.conf.set('spark.sql.crossJoin.enabled', 'true')
    return spark


class PA2Data(object):
    review_schema = T.StructType([
        T.StructField('reviewerID', T.StringType(), False),
        T.StructField('asin', T.StringType(), False),
        T.StructField('overall', T.FloatType(), False)
    ])
    product_schema = T.StructType([
        T.StructField('asin', T.StringType()),
        T.StructField('salesRank', T.StringType()),
        T.StructField('categories', T.StringType()),
        T.StructField('title', T.StringType()),
        T.StructField('price', T.FloatType()),
        T.StructField('related', T.StringType())
    ])
    product_processed_schema = T.StructType([
        T.StructField('asin', T.StringType()),
        T.StructField('title', T.StringType()),
        T.StructField('price', T.FloatType()),
        T.StructField('meanRating', T.FloatType()),
        T.StructField('countRating', T.FloatType()),
        T.StructField('category', T.StringType()),
        T.StructField('bestSalesCategory', T.StringType()),
        T.StructField('bestSalesRank', T.FloatType()),
        T.StructField('countAlsoBought', T.FloatType()),
        T.StructField('meanAlsoBought', T.FloatType()),
        T.StructField('countAlsoViewed', T.FloatType()),
        T.StructField('meanAlsoViewed', T.FloatType()),
        T.StructField('countBoughtTogether', T.FloatType()),
        T.StructField('meanBoughtTogether', T.FloatType()),
        T.StructField('countBuyAfterViewing', T.FloatType()),
        T.StructField('meanBuyAfterViewing', T.FloatType()),
        T.StructField('meanImputedPrice', T.FloatType()),
        T.StructField('meanImputedMeanRating', T.FloatType()),
        T.StructField('meanImputedMeanAlsoBought', T.FloatType()),
        T.StructField('meanImputedMeanAlsoViewed', T.FloatType()),
        T.StructField('meanImputedMeanBoughtTogether', T.FloatType()),
        T.StructField('meanImputedMeanBuyAfterViewing', T.FloatType()),
        T.StructField('medianImputedBestSalesRank', T.FloatType()),
        T.StructField('medianImputedCountRating', T.FloatType()),
        T.StructField('medianImputedCountAlsoBought', T.FloatType()),
        T.StructField('medianImputedCountAlsoViewed', T.FloatType()),
        T.StructField('medianImputedCountBoughtTogether', T.FloatType()),
        T.StructField('medianImputedCountBuyAfterViewing', T.FloatType()),
        T.StructField('unknownImputedTitle', T.StringType()),
        T.StructField('unknownImputedCategory', T.StringType()),
        T.StructField('unknownImputedBestSalesCategory', T.StringType())
    ])
    salesRank_schema = T.MapType(T.StringType(), T.IntegerType())
    categories_schema = T.ArrayType(T.ArrayType(T.StringType()))
    related_schema = T.MapType(T.StringType(), T.ArrayType(T.StringType()))
    schema = {
        'review': review_schema,
        'product': product_schema,
        'product_processed': product_processed_schema
    }
    metadata_schema = {
        'salesRank': salesRank_schema,
        'categories': categories_schema,
        'related': related_schema
    }

    def __init__(self,
                 spark,
                 path_dict,
                 output_root,
                 deploy
                 ):
        self.spark = spark
        self.path_dict = path_dict
        self.output_root = output_root
        self.deploy = deploy

    def load(self, name, path, infer_schema=False):
        schema = self.schema[name] if not infer_schema else None
        data = self.spark.read.csv(
            path,
            schema=schema,
            escape='"',
            quote='"',
            inferSchema=infer_schema,
            header=True
        )
        if name == 'product':
            for column, column_schema in self.metadata_schema.items():
                if column in data.columns:
                    data = data.withColumn(column, F.from_json(
                        F.col(column), column_schema))
        return data

    def load_all(self, input_format='dataframe', no_cache=False):
        data_dict = {}
        count_dict = {}
        for name, path in self.path_dict.items():

            data = self.load(name, path)
            if name == 'product_processed':
                data = data[['asin',
                             'unknownImputedTitle',
                             'unknownImputedCategory'
                             ]]. \
                    withColumnRenamed('unknownImputedTitle', 'title'). \
                    withColumnRenamed('unknownImputedCategory', 'category')
            if input_format == 'rdd':
                data = data.rdd
            if self.deploy and not no_cache:
                data = data.cache()
            data_dict[name] = data
            count_dict[name] = data.count() if not no_cache else None
        return data_dict, count_dict

    def save(self, res, task_name):
        if task_name in TASK_NAMES:
            df = self.spark.createDataFrame([res])
            output_path = os.path.join(
                self.output_root, task_name + EXT)
            df.coalesce(1).write.mode('overwrite').json(output_path)
        else:
            raise ValueError
