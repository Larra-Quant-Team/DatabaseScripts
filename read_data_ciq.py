# -*- coding: utf-8 -*-
"""
Created on Mon Jan  25 11:31:37 2021

@author: fipaniagua
"""
from datetime import datetime, timedelta
import os
import sys
import pandas as pd
import pickle as pkl
current_dir = os.getcwd()
#os.chdir('../ApiCiq')
sys.path.insert(0, '../ApiCiq')
from apicapitaliq import ApiCapitalIQ
#os.chdir('../Tables')
sys.path.insert(0, '../Tables')
import tables
os.chdir(current_dir)

# Get Mongo Collection object
eq = tables.EquityMaster()
keys = eq.get_keys()
key2_ls = eq.distinct('country')
res_query = eq.query(expand=False,  start="2019", end="2020")
print(keys)
print(key2_ls)
print(res_query)