# AMALGAMA Mongo API wrapper

Convenience mongo api for financial analysis

## SETUP

1- Create and define mongo_credentials.py in the same folder as mongo_api.py. Typically something like this:

```shell script
# in mongo_api/mongo_api
credentials = {
    "user": "user_name",
    "password": "user_password",
    "host": "your_db_host",
    "args": "test?retryWrites=true&w=majority",
    "port": 27017
}
```

2- install requirements:

```shell script
pip install -r requirements.txt  # in root folder
```

3- build distribution locally:

```shell script
python setup.py bdist_wheel --universal  # in root folder
```

4- install wheel in target env, check correct built version in previous step

```shell script
python -m pip install ..\path\to\mongo_api\dist\mongo_api-0.x.x-py2.py3-none-any.whl  # in target root folder
```

5- to uninstall in target env

```shell script
pip uninstall mongo_api  # in target root folder
```

## Class definitions

```shell script
class ExampleSeries(_MongoSeries):
    _name = 'example_timeseries'
    _key_ls = ['key1', 'key2', 'etc']
    _drop_weekends = True
    _override_boolean_key = ['key2']

class ExampleTable(_MongoTable):
    _name = 'example_table'
    _key_ls = ['iso_date']

class ExampleDoc(_MongoDoc):
    _name = 'example_doc'
    _key_ls = ['key1', 'key2', 'etc']
    _override_boolean_key = ['key1']

class _ExampleLog(_MongoLog):
    _name = 'example_log'
    _key_ls = ['log_name', 'iso_date']
```

## Examples

```shell script
coll = ExampleSeries(key1='key_val', key2='some_value')

key2_ls = coll.distinct('key2')

df = coll.query(rename=['key3', 'key4'], expand=True, start="2015", end="2018")

coll.insert(some_df, col_key=['key1', 'key2'])
```