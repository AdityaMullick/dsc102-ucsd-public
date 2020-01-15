from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import numpy as np
import os
import traceback
from math import isclose
SEED = 102
TASK_NAMES = ['task_' + str(i) for i in range(1, 7)]
EXT = '.json'


def test_deco(f):
    def f_new(*args, **kwargs):
        count = kwargs['count']
        failures = kwargs['failures']
        total_count = kwargs['total_count']
        test_name = kwargs['test_name']
        count += 1
        print ('Test {}/{} : {} ... '.format(count, total_count, test_name), end='')  # noqa
        try:
            f(*args)
            print('Pass')
        except Exception as e:
            failures.append(e)
            print('Fail: {}'.format(e))
            traceback.print_exc()
        return count
    return f_new


class PA2Test(object):
    def __init__(self, spark, test_results_root):
        self.spark = spark
        self.test_results_root = test_results_root
        self.dict_res = {}
        for task_name in TASK_NAMES:
            try:
                df = self.spark.read.json(
                    os.path.join(test_results_root, task_name + EXT), )
                self.dict_res[task_name] = df.collect()[0].asDict()
            except Exception as e:
                print(e)
                traceback.print_exc()

    def test(self, res, task_name):
        row = 79
        start_msg = 'tests for {} '.format(task_name)
        comp_row = max(0, row - len(start_msg))
        comp_dashes = ''.join(['-' * comp_row])
        print (start_msg + comp_dashes)
        failures = []
        count = 0
        ref_res = self.dict_res[task_name]
        total_count = len(ref_res)
        test_dict = {
                'count': count,
                'failures': failures,
                'total_count': total_count,
                'test_name': None
            }
        if task_name not in ['task_5', 'task_6']:
            for k, v in ref_res.items():
                test_name = k
                test_dict['test_name'] = test_name
                test_dict['count'] = count
                count = self.identical_test(test_name, res[k], v, **test_dict)

        elif task_name == 'task_5':
            total_length = 10
            test_dict['total_count'] = 8
            at_least = 1
            ref_res_identical = {
                k: v
                for k, v in ref_res.items()
                if k in ['count_total', 'size_vocabulary']
            }

            for k, v in ref_res_identical.items():
                test_name = k
                test_dict['test_name'] = test_name
                test_dict['count'] = count
                count = self.identical_test(test_name, res[k], v, **test_dict)

            for k in ['word_0_synonyms', 'word_1_synonyms', 'word_2_synonyms']:
                res_v = dict(res[k])
                res_r = dict(map(tuple, ref_res[k]))
                test_name = '{}-length'.format(k)
                test_dict['test_name'] = test_name
                test_dict['count'] = count
                count = self.identical_length_test(k, res_v, list(range(total_length)),
                                                   **test_dict)

                test_name = '{}-correctness'.format(k)
                test_dict['test_name'] = test_name
                test_dict['count'] = count
                count = self.synonyms_test(
                    res_v, res_r, total_length, at_least, **test_dict)

        elif task_name == 'task_6':
            total_count = 9
            test_dict = {
                'count': count,
                'failures': failures,
                'total_count': total_count,
                'test_name': None
            }
            ref_res_identical = {
                k: v
                for k, v in ref_res.items() if k in ['count_total']
            }
            for k, v in ref_res_identical.items():
                test_name = k
                test_dict['test_name'] = test_name
                test_dict['count'] = count
                count = self.identical_test(test_name, res[k], v, **test_dict)
            for k in ['meanVector_categoryOneHot', 'meanVector_categoryPCA']:
                res_v = np.abs(res[k])
                res_r = np.abs(ref_res[k])
                test_name = '{}-length'.format(k)
                test_dict['test_name'] = test_name
                test_dict['count'] = count
                count = self.identical_length_test(k, res_v, res_r,
                                                   **test_dict)

                for fname, fns in zip(('sum', 'mean', 'variance'),
                                      (np.sum, np.mean, np.var)):
                    test_name = '{}-{}'.format(k, fname)
                    test_dict['test_name'] = test_name
                    test_dict['count'] = count
                    vv = fns(res_v)
                    vr = fns(res_r)
                    count = self.identical_test(test_name, vv, vr, **test_dict)
        
        print('{}/{} passed'.format(total_count - len(failures), total_count))
        print (''.join(['-' * row]))
        return len(failures) == 0

    @test_deco
    def identical_length_test(self, k, v1, v2):
        size1 = len(v1)
        size2 = len(v2)
        assert size1 == size2, \
            'Length of {} must be {}, but got {} instead'.format(
                    k, size2, size1)

    @test_deco
    def identical_test(self, k, v1, v2):
        assert isclose(
                v1, v2, rel_tol=1e-7, abs_tol=0.0), \
                'Value of {} should be close enough to {}, but got {} instead'.format(
                    k, v2, v1)

    @test_deco
    def synonyms_test(self, res_v, res_r, total, at_least):
        if sum(res_v.values()) / len(res_v) < 0.9:
            print(
                "WARNING: your top synonyms have an average score less than 0.9, this might indicate errors"
            )
        correct = len(set(res_v.keys()).intersection(set(res_r.keys())))
        assert correct >= at_least, \
                'At least {} synonyms out of {} should overlap with our answer, got only {} instead'. \
                format(at_least, total, correct)


def spark_init(pid):
    spark = SparkSession.builder.appName(pid).getOrCreate()
    url = spark.conf.get(
        'spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES')
    print('Connect to Spark UI: {}'.format(url))
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
        print ("Loading datasets ...", end='')  # noqa
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
        print ("Done")
        return data_dict, count_dict

    def save(self, res, task_name):
        if task_name in TASK_NAMES:
            df = self.spark.createDataFrame([res])
            output_path = os.path.join(
                self.output_root, task_name + EXT)
            df.coalesce(1).write.mode('overwrite').json(output_path)
        else:
            raise ValueError
