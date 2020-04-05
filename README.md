# LegAnonymizer
A generic automated utility for anonimizing PySpark DataFrames. 

*Please do think about your own requirements and anonymization needs before blindly using this and publishing data. The LegAnonymizer is not a full fool-proof anonymizer which adheres to GDPR regulations for publishing personal data. The LegAnonymizer is more of a tool which performs Pseudonymization in order to keep the distribution and semantic meaning for analysis and visualization purposes.* 

## Instructions for use

### Installation

This must be run in a virtualenvironment with the correct dependencies installed. These are enumerated in `requirements.txt`

#### Install `virtualenv` globally:

```
[sudo] pip install virtualenv
```

Create a virtualenv and install the dependencies of `anonymize-it`
```
virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
```

and run:

```
python anonymize.py configs/config.json
```

### Quick Start
Import the anonymizer class in the top of your file using:
```
import LegAnonymizer
```

Create an instance of the LegAnonymizer class. Either with default settings or with particular parameters as you wish.
```
leg_anonymizer = LegAnonymizer(language='en', mode='safe')
```
The language parameter takes all languages that Faker takes (https://faker.readthedocs.io/en/master/) this includes 'en', 'fr_FR', 'es_ES' amongst others.

The mode parameter takes the following two:
* 'safe': make sure that all unique values stay unique, much slower
* 'fast': a faster way, but for large sets doesn't guarantee uniqueness.

Finally, anonymize your PySpark DataFrame. Only mandatory to pass is your DataFrame.
```
df = anon.anonymize_data(df,anonymize_columns=None, column_types=None)
```
The other parameters can be set if you wish to specify your own columns to be anonymized:
* 'anonymize_columns': expects a list with column names to be anonymized
* 'column_types': expects a list of column types in same order as above, the below types are supported.

Supported column_types:
```
'address'
'creditcard'
'phone_number'
'date_time'
'e-mail_address'
'ipv4_address'
'mac_address'
'url'
'username'
'IBAN'
```
