import os
import sys
current_file = os.path.abspath(os.path.dirname(__file__))
parent_of_parent_dir = os.path.join(current_file, '../../')
sys.path.insert(0, parent_of_parent_dir)
from modules.mongo_api.mongo_api.mongo_api import _MongoSeries, _MongoDoc, _MongoTable


class EquityMaster(_MongoSeries):
    _name = 'equity_master'
    _key_ls = ['country', 'currency', 'asset', 'investable', 
               'ind_sector', 'ind_group', 'ind_industry',
               'ind_internal', 'ind_esg', 'field']
    _drop_weekends = True
    _override_boolean_key = ['investable']

class MacroMaster(_MongoSeries):
    _name = 'macro_master'
    _key_ls = ['country', 'currency', 'instrument', 'source', 'field']
    _drop_weekends = True 

class WebInfoMaster(_MongoSeries):
    _name = 'web_info_master'
    _key_ls = ['country', 'asset', 'investable', 
                'ind_sector', 'ind_group', 'ind_industry',
                'ind_internal', 'ind_esg',  'source', 'field']
    _drop_weekends = True

    