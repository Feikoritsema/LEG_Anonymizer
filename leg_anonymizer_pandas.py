from faker.providers import credit_card, phone_number, date_time, internet, bank
import pandas as pd
from faker import Faker
from sklearn import preprocessing
from collections import defaultdict
import time

class UniqueFaker:
    def __init__(self, col_type, types):
        self.col_type = col_type
        self.types = types
        self.generated = set()

    def __call__(self):
        fakes = self.types[self.col_type]
        value = fakes()
        while value in self.generated:
            value = fakes()
        # We now have a unique value
        self.generated.add(value)
        return value


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


def recognize_columns(df):
    '''
    This function offers a very naive approach to recognizing the 'to-be-anonymized' columns.
    :param df: A Spark DataFrame
    :return: A list with columns and column types that should be anonymized
    '''
    column_names = df.columns
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


class LegAnonymizerPandas:
    '''
    Main Vanilla Python LEG-Anonymizer class.
    '''
    def __init__(self, language='en', mode='fast'):
        self.mode = mode
        self.fake = Faker(language)
        self.fake.add_provider(credit_card)
        self.fake.add_provider(phone_number)
        self.fake.add_provider(date_time)
        self.fake.add_provider(internet)
        self.fake.add_provider(bank)
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
        if anonymize_columns is None:
            df_sample = df.sample(2)
            anonymize_columns, column_types = recognize_columns(df_sample)

        # Safe mode
        if self.mode == 'fast':
            # Generate
            for idx in range(len(anonymize_columns)):
                col = anonymize_columns[idx]
                col_type = column_types[idx]
                faker_mapper = defaultdict(self.possible_types[col_type])
                mapper = lambda x: faker_mapper[x]
                df[col] = df[col].apply(mapper)

        elif self.mode == 'safe':
            for idx in range(len(anonymize_columns)):
                col = anonymize_columns[idx]
                col_type = column_types[idx]
                faker_mapper = defaultdict(UniqueFaker(col_type, self.possible_types))
                mapper = lambda x: faker_mapper[x]
                df[col] = df[col].apply(mapper)

        return df
