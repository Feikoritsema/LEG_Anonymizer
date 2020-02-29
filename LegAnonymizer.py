from faker import Faker
from faker.providers import credit_card, phone_number, date_time, internet, bank
import pyspark
from pyspark.ml.feature import StringIndexer
from pyspark import SparkContext
from pyspark.sql import SQLContext
from collections import defaultdict
from pyspark.sql.functions import udf, col, countDistinct
import time
import logging
import findspark


def generate_unique_fakes(n, faker_dict):
    '''
    Generates unique fake values
    :param n: The amount of fake values required
    :param faker_dict: A defaultdict object with Faker generator.
    :return: returns a list of unique fake values for the particular type.
    '''
    fake_values = set()
    counter = 0
    safety_guard = n
    while len(fake_values) < n + 1:
        if counter % (int(n / 10)) == 0:
            # Built-in to make sure we don't get caught in an infinite loop where no more fake values are available
            if len(fake_values) == safety_guard:
                print('Not enough fake values could be generated within limits, please use fast mode.. ')
                exit()
            safety_guard = len(fake_values)
        fake_values.add(faker_dict[counter])
        counter += 1
    return list(fake_values)


def recognize_columns_spark(df):
    '''
    This function offers a very naive approach to recognizing the 'to-be-anonymized' columns.
    :param df: A Spark DataFrame
    :return: A list with columns and column types that should be anonymized
    '''
    column_names = df.schema.names
    keys = ['name', 'address', 'credit', 'phone', 'date', 'mail', 'ip', 'mac', 'url', 'user', 'iban']
    types = ['name', 'address', 'creditcard', 'phone_number', 'date_time', 'e-mail_address', 'ipv4_address',
             'mac_address', 'url', 'username', 'IBAN']
    anonymize_columns = []
    column_types = []
    for column_name in column_names:
        for idx in range(len(keys)):
            if keys[idx] in column_name.lower():
                anonymize_columns.append(column_name)
                column_types.append(types[idx])
    return anonymize_columns, column_types


class LegAnonymizer():
    '''
    Main LEG-Anonymizer class.
    '''
    def __init__(self, language='en', mode='safe'):
        self.fake = Faker(language)
        self.fake.add_provider(credit_card)
        self.fake.add_provider(phone_number)
        self.fake.add_provider(date_time)
        self.fake.add_provider(internet)
        self.fake.add_provider(bank)
        self.mode = mode
        self.possible_types = {'name': self.fake.name,
                               'address': self.fake.address,
                               'creditcard': self.fake.credit_card_number,
                               'phone_number': self.fake.phone_number,
                               'date_time': self.fake.date_time,
                               'e-mail_address': self.fake.free_email,
                               'ipv4_address': self.fake.ipv4,
                               'mac_address': self.fake.mac_address,
                               'url': self.fake.url,
                               'username': self.fake.user_name,
                               'IBAN': self.fake.iban}

    def anonymize_data(self, df, anonymize_columns=None, column_types=None):
        '''
        Main function, returns anonymized df
        :param df: To be anonymized df
        :param anonymize_columns: Optional; if certain columns are known to be anonymized
        :param column_types: Optional: needs to be defined if above is defined
        :return: Anonymized df
        '''

        print('Analyzing columns...')
        if anonymize_columns is None:
            df_sample = df.limit(10)
            anonymize_columns, column_types = recognize_columns_spark(df_sample)
        print('Columns that will be anonymized are: ' + " ".join([str(elem) for elem in anonymize_columns]))
        print('These columns have the following types: ' + " ".join([str(elem) for elem in column_types]))

        if self.mode == 'fast':
            # Generate
            print('Generating fake values...')
            for idx in range(len(anonymize_columns)):
                column_name = anonymize_columns[idx]
                print('For column ' + column_name)
                col_type = column_types[idx]
                faker_mapper = defaultdict(self.possible_types[col_type])
                mapper = udf(lambda x: faker_mapper[x])
                df = df.withColumn(column_name, mapper(df[column_name]))

        elif self.mode == 'safe':
            print('Indexing columns...')
            for column in anonymize_columns:
                print(column)
                indexer = StringIndexer(inputCol=column, outputCol=column+"index")
                print('Starting fit and transform for column ' + column)
                df = indexer.fit(df).transform(df)
                print('Dropping column ' + column)
                df = df.drop(column).collect()
                print('Renaming column ' + column + 'index to ' + column)
                df = df.withColumnRenamed(column + 'index', column)

            # Generate
            print('Generating fake values...')
            for idx in range(len(anonymize_columns)):
                column_name = anonymize_columns[idx] + 'index'
                print('For column: ' + column_name)
                col_type = column_types[idx]
                faker_mapper = defaultdict(self.possible_types[col_type])
                max_row = df.agg({column_name: "max"}).collect()[0]
                n = max_row["max(" + column_name + ")"]
                fake_mapper = generate_unique_fakes(n, faker_mapper)
                mapper = udf(lambda x: fake_mapper[int(x)])
                df = df.withColumn(column_name, mapper(df[column_name]))

        print('Done.')
        return df


if __name__ == '__main__':
    '''
    If you want to run any tests, run them here.
    '''
    findspark.init()
    sc = SparkContext("local", "LEG_Anonymizer")
    sqlContext = SQLContext(sc)
    df = sqlContext.read.parquet('data/fake_data2_5m.parquet.gzip')
    print('Amount of distinct Names: ' +  str(df.select("Name").distinct().count()))
    print('Amount of distinct Addresses: ' + str(df.select("Address").distinct().count()))
    print('Shape of DataFrame: ' + str((df.count(), len(df.columns))))
    print(df.show())
    start_proc2 = time.time()
    anon = LegAnonymizer(mode='fast')
    df = anon.anonymize_data(df)
    print('Anonymization took: ' + str(time.time()-start_proc2) + ' seconds')
    print('Amount of distinct Names: ' + str(df.select("Name").distinct().count()))
    print('Amount of distinct Addresses: ' + str(df.select("Address").distinct().count()))
    print('Shape of DataFrame: ' + str((df.count(), len(df.columns))))
    print(df.show())

