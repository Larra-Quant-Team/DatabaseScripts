# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 17:38:49 2021

@author: aback
"""
from datetime import datetime, timedelta
import os
import pandas as pd
import pickle as pkl
import sys

# local packages
from modules.apiciq.apicapitaliq import ApiCapitalIQ
from modules.tables import tables



eq = tables.EquityMaster(asset="1", currency="Local", field="IQ_CLOSEPRICE")
keys = eq.get_keys()
qry = eq.query()
print(qry)