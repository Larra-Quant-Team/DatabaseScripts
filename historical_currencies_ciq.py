from datetime import datetime, timedelta
import os
import pandas as pd
import pickle as pkl
import sys
from time import time, sleep
import json

# local packages
from mailer_quant import Mailer
from prettifier import HtmlConverter
from modules.apiciq.apicapitaliq import ApiCapitalIQ
from modules.tables import tables

current_dir = os.getcwd()
os.chdir(current_dir)


def create_key(region, currency, field, source="CIQ"):
    country = region
    currency = currency
    instrument = "FX_USD"
    source = source
    field = field
    return '.'.join([country, currency, instrument, source, field])


#Load Mongo Table
eq = tables.MacroMaster()
keys = eq.get_keys()

# Create Api object
api = ApiCapitalIQ()

# Load Macro
dbpath = './files/'
macros = pd.read_excel(dbpath + 'Company_Base_Definitivo.xlsx',
                          sheet_name='Macro',  engine='openpyxl')
macros.set_index('Region', inplace=True)
macros.sort_index(inplace=True)
macros = macros.dropna()

start = datetime(2010, 1, 1)
start_str = start.strftime('%Y-%m-%d')
end = (datetime.today() - timedelta(1))
end_str = end.strftime('%Y-%m-%d')
# TODO check properties are correct
properties = {'StartDate': start_str,
                   'EndDate': end_str,}

logs = ""

for id_q, macro in macros.iterrows():
    requests = []
    ticker = f"{macro.Currency_Id}USD"
    requests.extend([api.historical_value(macro.Currency_Id ,"IQ_CLOSEPRICE", properties)])
    print(macro.name)
    response = api.sendRequest(requests).json()
    #print(response.json())
    df = []
    for mnemo_data in response['GDSSDKResponse']:
            field = mnemo_data['Mnemonic']
            if mnemo_data['ErrMsg'] != '':
                logs += ('ID {} tiene error en campo {} \n'
                         .format(id_q, field))
                continue
            rows = [list(x.values())[0] for x in mnemo_data['Rows']]
            if field == 'IQ_FILINGDATE_IS':
                rows = {datetime.strptime(y[1], '%m/%d/%Y'):
                        datetime.strptime(y[0], '%b %d %Y %H:%M%p')
                        for y in rows if y[0] not in ['Data Unavailable',
                                                      'CapabilityNeeded']}
            else:
                rows = {datetime.strptime(y[1], '%m/%d/%Y'):
                        float(y[0])
                        for y in rows if y[0] not in ['Data Unavailable',
                                                      'CapabilityNeeded']}
            if len(rows) != 0:
                series = pd.Series(rows)
                series.name = create_key(macro.name, macro.Currency, field)
                df.append(series)
            else:
                logs += 'ID {} tiene error en campo {} , puede ser "CapabilityNeeded \n'.format(id_q, field)
    df = pd.concat(df, axis=1)
    #print(df)
    eq.insert(df, keys)