import pandas as pd
from faker import Faker
from faker.providers import credit_card
from faker.providers import phone_number
from faker.providers import date_time
from faker.providers import python
import random
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
    numrows = 1000000
    columns = ['Name', 'Credit_card', 'Telephone', 'Address', 'Target', 'date_time']
    df = pd.DataFrame(columns=columns)
    for col in df.columns:
        df[col] = range(numrows)
    for col in df.columns:
        print(col)
        faker_mapper = defaultdict(possible_types[col])
        mapper = lambda x: faker_mapper[x]
        df[col] = df[col].apply(mapper)
    print(df)
    df.to_parquet('fake_data.parquet.gzip', compression='gzip')

