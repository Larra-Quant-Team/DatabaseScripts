import datetime as dt
import pandas as pd
import itertools
import pymongo  # requires dnspython
import configparser
from .mongo_credentials import credentials

CREDENTIALS = credentials


class _MongoAbstract(object):
    _instance = None
    _credentials = None
    _name = None
    _key_ls = None
    _del_many_limit = 50
    _override_boolean_key = None

    def __init__(self, instance='remote', **query):
        self._set_instance(instance)
        self._set_credentials()

        if not isinstance(self._name, str) or not isinstance(self._instance, str) or not isinstance(self._key_ls, list):
            raise Exception('Instance (str), collection _name (str) and _key_ls (list) need to be properly declared!')

        db_obj = self._get_db_object()
        collection_names_ls = db_obj.list_collection_names()

        idx_tup_ls = [(i, pymongo.ASCENDING) for i in self._key_ls]
        if self._name not in collection_names_ls:
            print(f"Creating new {self._instance} collection: {self._name}\n")
            collection = db_obj[self._name]  # this will create the collection
            collection.create_index(idx_tup_ls, unique=True, name=self._name)  # this will index it
        else:
            collection = db_obj[self._name]  # collection already exists

        self._db_obj = db_obj
        self._collection = collection
        query = self._override_boolean_keys(**query)
        self._is_adv_query = sum([isinstance(v, list) for v in query.values()])
        self._set_query(query)
        self._set_advanced_query()
        self._out = None
        self._cursor = self._get_cursor()
        self._full_key_ls = self._key_ls + ['values']

    def _set_credentials(self):
        if self._credentials is None:
            self._credentials = CREDENTIALS

    def _get_credentials(self):
        if self._credentials is None:
            self._set_credentials()
        return self._credentials

    def _set_instance(self, instance):
        self._instance = instance

    def _override_boolean_keys(self, **query):
        if self._get_boolean_keys() and query:
            for key in self._get_boolean_keys():
                if key in query.keys():
                    if isinstance(query[key], (tuple, list)):
                        query[key] = [str(int(i)) if isinstance(i, bool) else i for i in query[key]]
                    else:
                        if isinstance(query[key], bool):
                            query[key] = str(int(query[key]))
        return query

    def _get_boolean_keys(self):
        if self._override_boolean_key is not None:
            if isinstance(self._override_boolean_key, str):
                return [self._override_boolean_key]
            elif isinstance(self._override_boolean_key, list):
                return self._override_boolean_key
            else:
                raise Exception(f'_override_boolean_key not recognized {self._override_boolean_key}')
        else:
            return None

    def __repr__(self):
        print_str = f'database: {self._get_db_name()}'
        print_str += f'\ninstance: {self._get_instance()}'
        print_str += f'\ncollection: {self._get_name()}'
        key_str = ', '.join([i for i in self.get_keys() if i != 'values'])
        print_str += f'\nkeys: [{key_str}]'
        storage_size = self._db_obj.command('collstats', self._get_name())['size'] / 1e6
        print_str += f'\nstorage size: {storage_size:.1f}MB'
        num_docs = self._get_cursor().count()
        print_str += f'\ndocuments: {num_docs}'
        return print_str

    def _get_collection(self):
        return self._collection

    def _get_instance(self):
        return self._instance

    def _get_name(self):
        return self._name

    def _get_host_and_port(self):
        user = self._get_credentials()['user']
        password = self._get_credentials()['password']
        host = self._get_credentials()['host']
        port = self._get_credentials()['port']
        full_hostname = f"mongodb+srv://{user}:{password}@{host}/{port}"
        return full_hostname, port

    def _get_db_object(self):
        client = pymongo.MongoClient(host=self._get_host_and_port()[0], port=self._get_host_and_port()[1])
        return client[self._get_db_name()]

    def _get_db_name(self):
        return self._get_credentials()['dbname']

    def _set_advanced_query(self):
        if self._is_adv_query:
            and_ls = []
            ls_dd = self._product_dict(**self._query)
            for dd in ls_dd:
                and_ls.extend([{'$and': [dd]}])
            or_dd = {'$or': and_ls}
            self._query = or_dd

    def _get_query(self):
        return self._query

    def _get_cursor(self):
        if self._out is None:
            self._cursor = self._collection.find(self._query)
        return self._cursor

    @staticmethod
    def _product_dict(**kwargs):
        for key, val in kwargs.items():  # make sure adv query values are all lists
            if not isinstance(val, list):
                kwargs.update({key: [val]})

        keys = kwargs.keys()
        vals = kwargs.values()
        for instance in itertools.product(*vals):
            yield dict(zip(keys, instance))

    def _set_query(self, query):
        query_dd = dict()
        for k, v in query.items():
            if v is not None:  # drop None from queries, but allow it to pass
                if isinstance(v, str) and '~' in v:
                    query_dd[k] = {'$ne': v.replace('~', '')}
                elif isinstance(v, list):
                    new_ls = list()
                    neg_ls = list()
                    for i in v:
                        if isinstance(i, str) and '~' in i:
                            neg_ls.append(i.replace('~', ''))
                        else:
                            new_ls.append(i)
                    if len(neg_ls) > 0:
                        new_ls.append({'$nin': neg_ls})
                    query_dd[k] = new_ls
                else:
                    query_dd[k] = v
        self._query = query_dd

    def drop_collection(self, confirm=''):
        if confirm != 'confirm':
            print('drop_collection not confirmed. Make sure you know what you are doing. Pass "confirm" to drop.')
        else:
            db_obj = self._get_db_object()
            db_obj.drop_collection(self._name)
            print('Database: %s' % self._get_db_name())
            print('Collection dropped: %s' % self._name)

    def _reindex(self, confirm=''):
        if confirm != 'confirm':
            print('reindex not confirmed. Nothing done. Pass "confirm" to reindex.')
        else:
            collection = self._get_collection()
            collection.drop_indexes()
            idx_tup_ls = [(i, pymongo.ASCENDING) for i in self._key_ls]
            collection.create_index(idx_tup_ls, unique=True, name=self._name)  # this will index it
            print('collection %s successfully re-indexed' % self._name)

    def count(self):
        print(f'Database: {self._get_db_name()}')
        print(f'Collection: {self._name}')

        print(f'Query: {self._query}')
        print(f'Documents: {self._collection.count_documents(self._query)}')

    def replace_field_value(self, existing_field=True, **kwargs):
        """
        make sure you declare the appropriate key=value arguments to be modified in your query!
        """
        if len(kwargs) != 1:
            raise Exception('please rename only one field at a time')

        field = list(kwargs.keys())[0]
        value = kwargs[field]

        if field not in self.get_keys() and existing_field:  # check init that everything is ok to reindex
            raise Exception('new field needs to be declared in collection keys before replace / re-index')

        if field == 'values':
            raise Exception('values cannot be replace with this method. Use .update_values() instead')

        if existing_field:
            if not self._query:
                raise Exception('Cannot replace key value without filters.')
            if self._get_boolean_keys() and field in self._get_boolean_keys():
                value = str(int(value))

        cursor = self._get_cursor()
        request_ls = []
        for doc in cursor:
            if existing_field:
                doc.pop(field)
            update_dd = {'$set': {field: value}}
            request = pymongo.UpdateOne(doc, update_dd, upsert=False)
            request_ls.append(request)

        if len(request_ls) > 0:
            self._get_collection().bulk_write(request_ls)
            print(f'collection field {field} successfully updated to {value}')

            if not existing_field:
                self._reindex('confirm')
        else:
            print('no operations to handle')

    @classmethod
    def get_keys(self):
        return [i for i in self._key_ls]  # pass a copy

    def get_key_values(self):
        match_dd = {'$match': self._query}

        key_dd = {k: f'${k}' for k in self._key_ls}
        group_dd = {'$group': {'_id': key_dd}}

        cursor = self._collection.aggregate([match_dd, group_dd])
        ls = [i['_id'] for i in list(cursor)]
        keys_df = pd.DataFrame(ls)
        if not keys_df.empty:
            keys_df = keys_df.sort_values(by=self._key_ls)
            keys_df = keys_df.reset_index(drop=True)
        return keys_df

    def _is_local_instance(self):
        return self._get_instance() == 'local'

    def delete_many(self, confirm=''):
        if not self._query:
            raise Exception('Cannot delete_many without filters. Use drop_collection() instead for a full drop.')

        lmt = self._del_many_limit
        cnt = self._collection.count_documents(self._query)
        if (cnt > lmt) and confirm != 'confirm':
            raise Exception(f'confirm delete_many() if more than {lmt} documents. Potentially deleted: {cnt}')

        before = self._collection.count_documents({})
        self._get_collection().delete_many(self._query)
        after = self._collection.count_documents({})
        self._out = None
        print(f'{before - after} document(s) deleted')

    def distinct(self, field):
        if isinstance(field, str):
            field = field
            cursor = self._get_cursor()
            return cursor.distinct(field)  # faster
        elif isinstance(field, list):
            group_dd = {"_id": {i: f'${i}' for i in field}}
            if len(self._query) > 0:
                cursor = self._collection.aggregate([{'$match': self._query},
                                                     {"$group": group_dd}])
            else:
                cursor = self._collection.aggregate([{"$group": group_dd}])
            dd = [doc['_id'] for doc in cursor]
            return pd.DataFrame().from_dict(dd)


class _MongoSeries(_MongoAbstract):
    _drop_weekends = None

    def __init__(self, instance='remote', **query):
        _MongoAbstract.__init__(self, instance=instance, **query)

    def _get_drop_weekends(self):
        return self._drop_weekends

    def _update_query_date_range(self, start, end):
        if start is not None and end is None:
            self._query['values.date'] = start
            # self._query['values'] = {'$elemMatch': {'date': {'$gte': start}}}
        elif start is None and end is not None:
            self._query['values.date'] = {'$lte': end}
        elif start is not None and end is not None:
            self._query['values.date'] = {'$gte': start, '$lte': end}
        else:  # both none
            pass

    def _get_query_cursor(self, start, end):
        if start is None and end is None:
            return self._get_cursor()  # simple case
        else:
            co = self._get_collection()
            key_ls = self.get_keys()
            match_dd = {'$match': self._query}
            if start is not None and end is None:
                if isinstance(start, (dt.datetime, dt.date)):
                    start = start.strftime('%Y%m%d')
                start = start.replace('-', '')
                filter_dd = {
                    '$filter':
                        {'input': '$values',
                         'as': 'vals',
                         'cond': {'$gte': ['$$vals.date', start]}
                         }
                }
            elif start is None and end is not None:
                if isinstance(end, dt.datetime):
                    end = end.strftime('%Y%m%d')
                end = end.replace('-', '')
                filter_dd = {
                    '$filter':
                        {'input': '$values',
                         'as': 'vals',
                         'cond': {'$lte': ['$$vals.date', end]}
                         }
                }
            else:  # both defined
                if isinstance(start, dt.datetime):
                    start = start.strftime('%Y%m%d')
                start = start.replace('-', '')

                if isinstance(end, dt.datetime):
                    end = end.strftime('%Y%m%d')
                end = end.replace('-', '')
                filter_dd = {
                    '$filter':
                        {'input': '$values',
                         'as': 'vals',
                         'cond': {'$and': [{'$gte': ['$$vals.date', start]},
                                           {'$lte': ['$$vals.date', end]}]}
                         }
                }
            key_dd = {i: 1 for i in key_ls}
            key_dd['values'] = filter_dd
            project_dd = {'$project': key_dd}
            cursor = co.aggregate([match_dd, project_dd])
            return cursor

    def insert(self, pd_obj, col_key):
        if pd_obj.empty:
            print('pd_obj empty, no data to insert')
            return

        if isinstance(pd_obj, pd.Series):
            pd_obj = pd.DataFrame(pd_obj)

        # check no dups
        if pd_obj.columns.has_duplicates:
            raise Exception('inserted object cannot have duplicated keys!')

        # initial checks
        if isinstance(col_key, str):
            col_key = [col_key]

        if len(self._key_ls) != len(col_key):
            name = self._name
            full = len(self._key_ls)
            given = len(col_key)
            raise Exception(f'insert error: {name}. col_key must have {full} elements. provided was {given}')

        df = pd_obj.copy()
        df.index = df.index.map(lambda i: i.strftime('%Y%m%d'))

        # integrity check
        for col in pd_obj.columns:
            col_val_ls = col.split('.')
            if len(self._full_key_ls) - 1 != len(col_val_ls):
                raise Exception(f'all columns must have {len(col_key)} elements. provided was {col_val_ls}')

        # do not store full NaN rows or columns
        df = df.dropna(how='all', axis=0)
        df = df.dropna(how='all', axis=1)

        request_ls = []
        for col in df.columns:
            col_val_ls = col.split('.')
            tup = zip(col_key, col_val_ls)
            query_dd = {k: v for k, v in tup}
            values_dd = df[col].dropna().to_dict()
            values_ls = [{'date': k, 'value': v} for k, v in values_dd.items()]
            update_dd = {'$set': {'values': values_ls}}
            request = pymongo.UpdateOne(query_dd, update_dd, upsert=True)
            request_ls.append(request)
        self._get_collection().bulk_write(request_ls)

    def last_update(self, how='each', order='min'):
        match_dd = {'$match': self._query}
        key_dd = {k: f'${k}' for k in self._key_ls}
        group_dd = {
            '$group':
                {'_id':
                     {'_id': key_dd},
                 'max': {'$max': "$values.date"}
                 }
        }
        sort_dd = {'$sort': {'values.date': 1}}
        cursor = self._get_collection().aggregate([match_dd, group_dd, sort_dd])
        last_df = pd.DataFrame()
        for num, doc in enumerate(cursor):
            dd = doc['_id']['_id']
            dd['last_update'] = doc['max'][-1]
            tmp_df = pd.DataFrame.from_dict(dd, orient='index').T
            last_df = pd.concat([last_df, tmp_df], axis=0, sort=True)
        last_df = last_df[self._key_ls + ['last_update']]
        last_df['last_update'] = last_df['last_update'].apply(pd.to_datetime)
        days_f = lambda x: (dt.datetime.today() - x).days
        last_df['last_update_days'] = last_df['last_update'].apply(days_f)
        if how == 'all':
            if order == 'max':
                return last_df['last_update'].max()
            elif order == 'min':
                return last_df['last_update'].min()
            else:
                raise Exception(f'argument order not recognized {order}')
        elif how == 'each':
            return last_df
        else:
            raise Exception(f'argument how not recognized: {how}')

    def query(self, start=None, end=None, rename=None, expand=False):
        """
        override is for self.last_update()
        """
        if self._out is None:
            cursor = self._get_query_cursor(start, end)

            if rename is not None:
                if isinstance(rename, str):
                    rename = [rename]

            exclude_ls = ['_id', 'values']
            df = pd.DataFrame()
            for doc in cursor:
                if len(doc['values']) > 0:
                    if rename is not None:
                        name = '.'.join([doc[i] for i in rename])
                    else:
                        name = '.'.join([doc[i] for i in sorted(doc.keys()) if i not in exclude_ls])
                    doc_df = pd.DataFrame().from_dict(doc['values']).set_index('date')
                    doc_df.columns = [name]
                    df = pd.concat([df, doc_df], axis=1, sort=True)

            if df.empty:
                return df

            df.index = df.index.map(pd.to_datetime)

            if self._get_drop_weekends():
                week_days_ls = df.index.weekday < 5
                df = df.loc[week_days_ls].copy()

            if expand:
                df.columns = df.columns.str.split('.', expand=True)

            # return series if only one column or row
            if len(df.columns) == 1:
                df = df[df.columns[0]].copy()

            if len(df.index) == 1:
                df = df.iloc[-1].copy()

            self._out = df.copy()
        return self._out

    def drop_datapoint(self, date_str):
        if self._collection.count_documents(self._query) == 0:
            raise Exception('no documents found with given filters')

        if self._collection.count_documents(self._query) > 1:
            raise Exception('you can only drop datapoints one series at a time')

        if isinstance(date_str, dt.datetime):
            date_str = date_str.strftime('%Y%m%d')

        cursor = self._get_cursor()
        request_ls = []
        for doc in cursor:
            doc.pop('_id')
            doc.pop('values')
            doc['values.date'] = date_str
            update_dd = {'$unset': {'values.$.value': ""}}
            request = pymongo.UpdateOne(doc, update_dd, upsert=False)
            request_ls.append(request)
        self._get_collection().bulk_write(request_ls)

    def update_values(self, pd_obj, col_key):
        if pd_obj.empty:
            print('pd_obj empty, no data to update')
            return

        # initial checks
        if isinstance(col_key, str):
            col_key = [col_key]

        if len(self._key_ls) != len(col_key):
            name = self._name
            full = len(self._key_ls)
            given = len(col_key)
            raise Exception(f'insert error: {name}. col_key must have {full} elements. provided was {given}')

        if isinstance(pd_obj, pd.Series):
            df = pd.DataFrame(pd_obj)
        else:
            df = pd_obj.copy()

        # check no dups
        if df.columns.has_duplicates:
            raise Exception('inserted object cannot have duplicated keys!')

        # integrity check
        for col in df.columns:
            col_val_ls = col.split('.')
            if len(self._full_key_ls) - 1 != len(col_val_ls):
                raise Exception(f'all columns must have {len(col_key)} elements. provided was {col_val_ls}')

        df.index = df.index.map(lambda i: i.strftime('%Y%m%d'))
        df = df.dropna(axis=0, how='all')
        df = df.dropna(axis=1, how='all')

        request_ls = []
        for col in df.columns:
            col_val_ls = col.split('.')
            tup = zip(col_key, col_val_ls)
            query_dd = {k: v for k, v in tup}
            values_dd = df[col].dropna().to_dict()

            # pull many
            date_ls = list(values_dd.keys())
            update_dd = {
                '$pull': {'values': {'date': {'$in': date_ls}}}
            }
            request = pymongo.UpdateMany(query_dd, update_dd, upsert=True)
            request_ls.append(request)

            # push many
            new_ls = [{'date': k, 'value': v} for k, v in values_dd.items()]
            update_dd = {'$push': {'values': {'$each': new_ls}}}
            request = pymongo.UpdateMany(query_dd, update_dd, upsert=True)
            request_ls.append(request)
        self._get_collection().bulk_write(request_ls, ordered=True)


class _MongoDoc(_MongoAbstract):
    def __init__(self, instance='remote', **query):
        _MongoAbstract.__init__(self, instance=instance, **query)

    def insert(self, dd):
        if not isinstance(dd, dict):
            print('insert dict empty, no data to insert')
            return

        # initial checks
        if sorted(list(self._get_query().keys())) != sorted(self.get_keys()):
            raise Exception(f'specify a full query to insert into a _MongoTable! Keys are: {str(self.get_keys())}')

        cast_key_ls = set([str(k) for k in dd.keys()])
        if len(cast_key_ls) != len(dd.keys()):
            raise Exception('identical string representations of keys are not allowed')
        insert_dd = {str(k): v for k, v in dd.items()}

        filter_dd = self._get_query()
        update_dd = {'$set': {'values': insert_dd}}
        self._get_collection().update_one(filter_dd, update=update_dd, upsert=True)

    def query(self, expand=False, rename=None):
        """
        override is for self.last_update()
        """

        if rename:
            if isinstance(rename, str):
                rename = [rename]

        if self._out is None:
            cursor = self._get_cursor()  # simple case

            df = pd.DataFrame()
            for num, doc in enumerate(cursor):
                if doc['values']:
                    doc.pop('_id')
                    val_dd = doc.pop('values')
                    if rename:
                        name = '.'.join([str(doc[k]) for k in rename])
                    else:
                        name = '.'.join([str(doc[k]) for k in sorted(doc.keys())])
                    s = pd.Series(val_dd, name=name, dtype=object)
                    df = pd.concat([df, s], axis=1, sort=True)

            if expand:
                df.columns = df.columns.str.split('.', expand=True)

            # return series if only one column or row
            if len(df.columns) == 1:
                df = df[df.columns[0]].copy()

            if len(df.index) == 1:
                df = df.iloc[-1].copy()

            self._out = df
        return self._out


class _MongoTable(_MongoAbstract):
    def __init__(self, instance='remote', **query):
        _MongoAbstract.__init__(self, instance=instance, **query)

    def insert(self, pd_obj):
        if pd_obj.empty:
            print('pd_obj empty, no data to insert')
            return

        if isinstance(pd_obj, pd.Series):
            pd_obj = pd.DataFrame(pd_obj)

        # check no dups
        if pd_obj.columns.has_duplicates:
            raise Exception('inserted object cannot have duplicated columns!')

        # initial checks
        if sorted(list(self._get_query().keys())) != sorted(self.get_keys()):
            raise Exception(f'specify a full query to insert into a _MongoTable! Keys are: {str(self.get_keys())}')

        # drop nan
        df = pd_obj.copy()
        df = df.dropna(how='all', axis=0)
        df = df.dropna(how='all', axis=1)

        if isinstance(df.columns, pd.core.indexes.multi.MultiIndex):
            df.columns = ['.'.join([i for i in col]) for col in df.columns]

        if isinstance(df.index, pd.core.indexes.multi.MultiIndex):
            df.index = ['.'.join([i for i in col]) for col in df.columns]

        df.columns = df.columns.map(str)
        df.index = df.index.map(str)

        values_dd = df.to_dict(orient='records')

        filter_dd = self._get_query()
        update_dd = {'$set': {'values': values_dd}}
        self._get_collection().update_one(filter_dd, update=update_dd, upsert=True)

    def query(self, rename=None, expand=False):
        """
        override is for self.last_update()
        """
        if self._out is None:
            cursor = self._get_cursor()  # simple case

            if rename is not None:
                if isinstance(rename, str):
                    rename = [rename]

            exclude_ls = ['_id', 'values']
            out_df = pd.DataFrame()
            cnt = 0
            for num, doc in enumerate(cursor):
                if len(doc['values']) > 0:
                    df = pd.DataFrame().from_records(doc['values'])
                    if rename is not None:
                        name = '.'.join([doc[i] for i in rename])
                    else:
                        name = '.'.join([doc[i] for i in sorted(doc.keys()) if i not in exclude_ls])
                    id_ls = [name] * df.shape[0]
                    df['_id'] = id_ls
                    out_df = pd.concat([out_df, df], axis=0, sort=True)
                    cnt += 1

            if out_df.empty:
                return out_df

            if cnt > 1:
                out_df = out_df.reset_index(drop=False)
                out_df = out_df.set_index('_id', drop=True)
                out_df.index.name = None
            else:
                out_df = out_df.drop('_id', axis=1)

            if expand:
                out_df.index = out_df.index.str.split('.', expand=True)

            # return series if only one column or row
            if len(out_df.columns) == 1:
                out_df = out_df[out_df.columns[0]].copy()

            if len(out_df.index) == 1:
                out_df = out_df.iloc[-1].copy()

            self._out = out_df.copy()
        return self._out


class _MongoLog(_MongoAbstract):
    def __init__(self, instance='remote', **query):
        _MongoAbstract.__init__(self, instance=instance, **query)

    def _log(self, df):
        request_ls = []
        if not df.empty:
            for (section, iso_dt), row_s in df.iterrows():
                values_dd = row_s.to_dict()
                doc_id = {'section': section,
                          'iso_date': iso_dt}
                update_dd = {'$push': {'values': values_dd}}
                request = pymongo.UpdateOne(doc_id, update_dd, upsert=True)
                request_ls.append(request)
            self._get_collection().bulk_write(request_ls)

    def query(self, rename=None, expand=False):
        """
        override is for self.last_update()
        """
        if self._out is None:
            cursor = self._get_cursor()  # simple case

            if rename is not None:
                if isinstance(rename, str):
                    rename = [rename]

            exclude_ls = ['_id', 'values']
            df = pd.DataFrame()
            for doc in cursor:
                if len(doc['values']) > 0:
                    if rename is not None:
                        name = '.'.join([doc[i] for i in rename])
                    else:
                        name = '.'.join([doc[i] for i in sorted(doc.keys()) if i not in exclude_ls])
                    doc_df = pd.DataFrame().from_records(doc['values'])
                    doc_df.index = [name] * len(doc_df.index)
                    df = pd.concat([df, doc_df], axis=0, sort=True)

            if df.empty:
                return df

            if expand:
                df.index = df.index.str.split('.', expand=True)

            # return series if only one column or row
            if len(df.columns) == 1:
                df = df[df.columns[0]].copy()

            if len(df.index) == 1:
                df = df.iloc[-1].copy()

            self._out = df.copy()
        return self._out
