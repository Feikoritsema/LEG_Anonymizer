from faker.providers import credit_card, phone_number, date_time, internet, bank
import pandas as pd
from faker import Faker
from collections import defaultdict
import time


def recognize_columns(df):
    column_names = df.columns
    anonymize_columns = []
    column_types = []
    for col in column_names:
        # For now just naive recognition approach
        if 'name' in col.lower():
            anonymize_columns.append(col)
            column_types.append('name')
        if 'address' in col.lower():
            anonymize_columns.append(col)
            column_types.append('address')
    return anonymize_columns,column_types


class VanillaPythonLegAnonymizer():
    '''
    Main Vanilla Python LEG-Anonymizer class.
    '''
    def __init__(self, language='en'):
        fake = Faker(language)
        self.possible_types = {'name': fake.name,
                               'address': fake.address}

    def anonymize_data(self, df, anonymize_columns=None, column_types=None):
        '''
        Main function, returns anonymized df
        :param df: To be anonymized df
        :param anonymize_columns: Optional; if certain columns are known to be anonymized
        :param column_types: Optional: needs to be defined if above is defined
        :return: Anonymized df
        '''
        if anonymize_columns is None:
            df_sample = df.sample(2)
            anonymize_columns, column_types = recognize_columns(df_sample)

        # Generate
        for idx in range(len(anonymize_columns)):
            col = anonymize_columns[idx]
            col_type = column_types[idx]
            faker_mapper = defaultdict(self.possible_types[col_type])
            mapper = lambda x: faker_mapper[x]
            df[col] = df[col].apply(mapper)

        return df

if __name__ == '__main__':
    '''
    If you want to run any tests, run them here.
    '''
    start_time = time.time()
    data = pd.read_parquet('data/fake_data2_5m.parquet.gzip')
    print('Reading the data took: ' + str(time.time()-start_time) + ' seconds')
    print(data.shape)
    print('Amount of distinct Names: ' + str(len(data['Name'].unique())))
    print('Amount of distinct Addresses: ' + str(len(data['Address'].unique())))
    print(data)
    start_proc1 = time.time()
    anon = VanillaPythonLegAnonymizer()
    data = anon.anonymize_data(data)
    print('Python way took: ' + str(time.time()-start_proc1) + ' seconds')
    print('Amount of distinct Names: ' + str(len(data['Name'].unique())))
    print('Amount of distinct Addresses: ' + str(len(data['Address'].unique())))
    print(data.shape)
    print(data)