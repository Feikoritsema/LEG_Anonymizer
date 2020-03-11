from pyspark import SparkContext
from pyspark.sql import SQLContext
import time
import findspark
from LegAnonymizer import LegAnonymizer
from leg_anonymizer_pandas import LegAnonymizerPandas
import pandas as pd


def run_pandas_test(filename):
    data = pd.read_parquet(filename)
    print('Amount of distinct Names: ' + str(len(data['Name'].unique())))
    start_time = time.time()
    anon = LegAnonymizerPandas(mode='safe')
    new_data = anon.anonymize_data(data)
    print('Safe Pandas Python Anonymization took: ' + str(time.time()-start_time) + ' seconds for filename: ' + filename)
    print('Amount of distinct Names: ' + str(len(new_data['Name'].unique())))
    start_time = time.time()
    anon = LegAnonymizerPandas(mode='fast')
    new_data = anon.anonymize_data(data)
    print('Fast Pandas Python Anonymization took: ' + str(time.time()-start_time) + ' seconds for filename: ' + filename)
    print('Amount of distinct Names: ' + str(len(new_data['Name'].unique())))


def run_spark_test(sqlContext, filename):
    df = sqlContext.read.parquet(filename)
    print('Amount of distinct Names: ' +  str(df.select("Name").distinct().count()))
    start_time = time.time()
    anon = LegAnonymizer(mode='fast')
    anon_df = anon.anonymize_data(df)
    print('Fast Spark Anonymization took: ' + str(time.time() - start_time) + ' seconds for filename ' + filename)
    print('Amount of distinct Names: ' + str(anon_df.select("Name").distinct().count()))
    start_time = time.time()
    anon = LegAnonymizer(mode='safe')
    anon_df = anon.anonymize_data(df)
    print('Safe Spark Anonymization took: ' + str(time.time()-start_time) + ' seconds for filename ' + filename)
    print('Amount of distinct Names: ' + str(anon_df.select("Name").distinct().count()))


if __name__ == '__main__':
    '''
    If you want to run any tests, run them here.
    '''
    data_folder = 'data/'
    file_names = ['fake_data10k', 'fake_data50k', 'fake_data100k', 'fake_data250k', 'fake_data500k',
                  'fake_data1M', 'fake_data2_5m']
    format = '.parquet.gzip'

    findspark.init()
    sc = SparkContext("local", "LEG_Anonymizer")
    sqlContext = SQLContext(sc)

    for file in file_names:
        full_file = data_folder + file + format
        run_spark_test(sqlContext, full_file)
        run_pandas_test(full_file)