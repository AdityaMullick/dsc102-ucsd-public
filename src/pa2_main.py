import argparse
import time
import traceback
import importlib
import os
from utilities import spark_init
from utilities import PA2Test
from utilities import PA2Data
from utilities import TASK_NAMES


class PA2Executor(object):
    def __init__(
        self,
        args,
        task_imls=None,
        input_format='dataframe',
        synonmys=['piano', 'rice', 'laptop'],
        output_pid_folder=False
    ):

        self.spark = spark_init(args.pid)
        path_dict = {
            'review': args.review_filename,
            'product': args.product_filename,
            'product_processed': args.product_processed_filename
            }

        self.task_imls = task_imls
        self.tests = PA2Test(self.spark, args.test_results_root)
        if output_pid_folder:
            output_root = os.path.join(args.output_root, args.pid)
        else:
            output_root = args.output_root
        self.data_io = PA2Data(self.spark, path_dict, output_root, deploy=True)

        self.data_dict, self.count_dict = self.data_io.load_all(
            input_format=input_format)
        self.task_names = TASK_NAMES
        self.synonmys = synonmys

    def arguments(self):
        arguments = {
            "task_1": [self.data_io, self.data_dict['review'], self.data_dict['product']],
            "task_2": [self.data_io, self.data_dict['product']],
            "task_3": [self.data_io, self.data_dict['product']],
            "task_4": [self.data_io, self.data_dict['product']],
            "task_5": [self.data_io, self.data_dict['product_processed']] + self.synonmys,
            "task_6": [self.data_io, self.data_dict['product_processed']]
        }
        return arguments

    def tasks(self):
        tasks = {
            "task_1": self.task_imls.task_1,
            "task_2": self.task_imls.task_2,
            "task_3": self.task_imls.task_3,
            "task_4": self.task_imls.task_4,
            "task_5": self.task_imls.task_5,
            "task_6": self.task_imls.task_6
        }
        return tasks

    def eval(self):
        arguments, tasks = self.arguments(), self.tasks()
        begin = time.time()
        results = []
        timings = []
        for task_name in self.task_names:
            task = tasks[task_name]
            fargs = arguments[task_name]
            sub_task_begin = time.time()
            result = self.eval_one(task, fargs, task_name)
            results.append(result)
            sub_task_end = time.time()
            sub_task_dur = sub_task_end - sub_task_begin
            timings.append(sub_task_dur)
            print ("{} time: {} sec".format(task_name, sub_task_dur))
        e2e_dur = time.time()-begin
        print ("End to end time: {} sec".format(e2e_dur))
        timings.append(e2e_dur)
        return results, timings

    def eval_one(self, task, fargs, task_name):
        result = False
        try:
            res = task(*fargs)
            result = self.tests.test(res, task_name)
        except Exception as e:
            print(
                "{} failed to execute, please inspect your code before submission. Exception: {}" \
                .format(task_name, e)
                )
            traceback.print_exc()
        return result

    def eval_by_name(self, task, task_name):
        arguments = self.arguments()
        fargs = arguments[task_name]
        self.eval_one(task, fargs, task_name)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--review_filename', type=str, default='s3://dsc102-pa2-public/dataset/user_reviews_train.csv'
    )
    parser.add_argument(
        '--product_filename', type=str, default='s3://dsc102-pa2-public/dataset/metadata_header.csv'
    )
    parser.add_argument(
        '--product_processed_filename', type=str,
        default='s3://dsc102-pa2-public/dataset/product_processed.csv'
    )
    parser.add_argument(
        '--test_results_root', type=str,
        default='s3://dsc102-pa2-public/test_results'
    )
    parser.add_argument(
        '--pid', type=str
    )
    parser.add_argument(
        '--module_name', type=str,
        default='assignment2'
    )
    parser.add_argument(
        '--output_root', type=str,
        default=None
    )
    parser.add_argument('--synonmys', nargs='+', type=str, default=['piano', 'rice', 'laptop'])
    args = parser.parse_args()
    if not args.output_root:
        args.output_root = 's3://{}-pa2/outputs'.format(args.pid)
    task_imls = importlib.import_module(args.module_name)
    pa2 = PA2Executor(args, task_imls, task_imls.INPUT_FORMAT, args.synonmys)
    results, timings = pa2.eval()
    res = []
    for task_name, result, timing in zip(TASK_NAMES, results, timings):
        res.append({'task_name': task_name,
                   'passed': result, 'time_sec': timing})
        pa2.data_io.save(res, 'summary')
