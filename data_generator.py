import pandas as pd
from faker import Faker
from faker.providers import credit_card, phone_number, date_time, python
from collections import defaultdict

faker = Faker()
faker.add_provider(credit_card)
faker.add_provider(phone_number)
faker.add_provider(date_time)
faker.add_provider(python)

possible_types = {'Name': faker.name,
                  'Credit_card': faker.credit_card_number,
                  'Telephone': faker.phone_number,
                  'Address': faker.address,
                  'Target': faker.pyint,
                  'date_time': faker.date_time}

if __name__ == '__main__':
    '''
    Used to create fake data with the below columns and below amount of rows.
    This can then be used in test_examples.py to test the results of pyspark vs pandas
    '''
    numrows = 2500000
    columns = ['Name', 'Credit_card', 'Telephone', 'Address', 'Target', 'date_time']
    df = pd.DataFrame(columns=columns)
    for col in df.columns:
        df[col] = range(numrows)
    for col in df.columns:
        faker_mapper = defaultdict(possible_types[col])
        mapper = lambda x: faker_mapper[x]
        df[col] = df[col].apply(mapper)
    df.to_parquet('fake_data2_5m.parquet.gzip', compression='gzip')

