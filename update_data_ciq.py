# -*- coding: utf-8 -*-
"""
Created on Mon Jan  26 15:31:37 2021

@author: fipaniagua
"""
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


def get_update_properties(asset, currency, fields, type):
    eq = tables.EquityMaster(currency=currency, asset=asset)
    last_update = eq.last_update()
    properties = {}
    for mnemonic in fields:
        # In the case mnemonic field has never been updated in the Database, the query start at 2010
        #print(mnemonic)
        start = datetime(2010, 1, 1)
        start_str = start.strftime('%Y-%m-%d')
        mnemonic_last_update = last_update.loc[last_update["field"] == mnemonic]
        if mnemonic_last_update.shape[0] > 0:
            start = mnemonic_last_update.last_update.item()
            start_str = start.strftime('%Y-%m-%d')
        #print(f"ultima vez consultado: {start_str}")
        end = (datetime.today() - timedelta(1))
        end_str = end.strftime('%Y-%m-%d')
        properties_d = {'StartDate': start_str,
                        'EndDate': end_str,
                        'currencyid': currency}
        properties_q = {'periodtype': 'IQ_CQ-{}'.format(4 *
                                                        (end.year - start.year)),
                        'metadatatag': 'perioddate',
                        'currencyid': currency}  
        if type == "quarter":
            properties[mnemonic] = {"quarter": properties_q}    
        else:           
            properties[mnemonic] = {"daily": properties_d}                   
    return properties


# Create Api object
api = ApiCapitalIQ()

# Load Companies
dbpath = './files/'
companies = pd.read_excel(dbpath + 'Company_Base_Definitivo.xlsx',
                          sheet_name='Compilado',  engine='openpyxl')
companies.set_index('ID_Quant', inplace=True)
companies.sort_index(inplace=True)
# Filter companies that aren't investable
companies = companies.loc[companies['Invertible'] == 1]

# Load CIQ Fields

fields = pd.read_excel(dbpath + 'Campos_SyP.xlsx',  engine='openpyxl')
fields = fields.loc[:, ['Campo_consulta', 'Periodicidad']]
quarter = fields.loc[fields['Periodicidad'] == 'Trimestral',
                     'Campo_consulta']
daily = fields.loc[fields['Periodicidad'] == 'Diaria',
                   'Campo_consulta']
daily_fields = daily.tolist()
quarter_fields = quarter.tolist()
print("d:", len(daily_fields))
print("q:", len(quarter_fields))

# Load previous state, if existent
try:
    with open(dbpath + 'temp/save_update_state.pkl', 'rb') as file:
        last_i = pkl.load(file)
except FileNotFoundError:
    last_i = -1

# Create log dictionary to build a json log file
logs = {}
initial_date = datetime.today()
program_start_time = time()
# Download Data from CIQ for all companies
for i, isin in enumerate(companies['ISIN']):
    if i > last_i:
        companie_log = {}
        ciq_q_log = {}
        if i % 25 == 0:
            print('{}. Consultando para {}'.format(i, isin))
        companie_info = companies.loc[companies['ISIN'] == isin]
        id_q = companie_info.index.item()
        # Create requests for each instrument
        
        requests_q = []
        start_time_load_q = time()
        properties_q = get_update_properties(str(id_q), "Local", quarter_fields, "quarter")
        end_time_load_q = time()
        requests_q.extend([api.historical_value(isin, mnemonic, properties_q[mnemonic]["quarter"])
                          for mnemonic in quarter_fields])
        response_q = api.sendRequest(requests_q)
        end_time_request_q = time()
        with open(dbpath + 'temp/historical_quarter_update_response_{}.pkl'.format(id_q),
                  'wb') as file:
            pkl.dump(response_q, file)
        end_time_dump_q = time()    
        requests_d = []
        properties_d = get_update_properties(str(id_q), "Local", daily_fields, "daily")
        #print(properties)
        requests_d.extend([api.historical_value(isin, mnemonic, properties_d[mnemonic]["daily"])
                          for mnemonic in daily_fields])
        response_d = api.sendRequest(requests_d)
        with open(dbpath + 'temp/historical_update_response_{}.pkl'.format(id_q),
                  'wb') as file:
            pkl.dump(response_d, file)    
        # Save current state
        with open(dbpath + 'temp/save_update_state.pkl', 'wb') as file:
            pkl.dump(i, file)    
        # logs
        ciq_q_log["Time Last Update"] = end_time_load_q - start_time_load_q
        ciq_q_log["Time CIQ request"] = end_time_request_q - end_time_load_q
        ciq_q_log["Time Dump"] = end_time_dump_q - end_time_request_q
        logs[i+1] = {"CIQ" : {"quarter" : ciq_q_log, "daly properties": properties_d,
                     "quarter properties": properties_q},
                     "fields": {}}


def create_key(company, currency, field):
    company[company.isna()] = 'null'
    country = company['Country']
    investible = str(company['Invertible'])
    asset = str(company.name)
    ind_sector = company['Industry_Sector']
    ind_group = company['Industry_Group']
    ind_industry = company['Industry']
    ind_internal = company['Internal_industry']
    ind_esg = company['ESG_Industry']
    
    return '.'.join([country, currency, asset, investible, ind_sector,
                     ind_group, ind_industry, ind_internal, ind_esg,
                     field])


# Get Mongo Collection object
eq = tables.EquityMaster()
keys = eq.get_keys()

# Load previous state, if existent
try:
    with open(dbpath + 'temp/save_update_state_load.pkl', 'rb') as file:
        last_id = pkl.load(file)
except FileNotFoundError:
    last_id = -1

for id_q, company in companies.iterrows():
    if id_q < last_id:
        continue
    if id_q % 25 == 0:
        print('Vamos en el id {}'.format(id_q))
    with open(dbpath + 'temp/historical_quarter_update_response_{}.pkl'.format(id_q),
              'rb') as file:
        response_q = pkl.load(file)
        if response_q.status_code != 200:
            logs[id_q]["err"] = ('ID {} tiene status code {} para data quarter \n'
                    .format(id_q, response_q.status_code))
        response_q = response_q.json()
     
    with open(dbpath + 'temp/historical_update_response_{}.pkl'.format(id_q),
              'rb') as file:
        response_d = pkl.load(file)
        if response_d.status_code != 200:
            logs[id_q]["err"] = ('ID {} tiene status code {} para data daily \n'
                    .format(id_q, response_d.status_code))
        response_d = response_d.json()
    
    df = []
    for res in [response_q, response_d]:
        for mnemo_data in res['GDSSDKResponse']:
            err = "0"
            field = mnemo_data['Mnemonic']
            currency = mnemo_data['Properties']['currencyid']
            msg = ""
            if mnemo_data['ErrMsg'] != '':
                err = "1"
                msg = ('ID {} tiene error en campo {} con currency {}\n'
                         .format(id_q, field, currency))
                msg += (f'{mnemo_data["ErrMsg"]} \n')         
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
                series.name = create_key(company, currency, field)
                df.append(series)
            else:
                err = "1"
                msg += 'ID {} tiene error en campo {} con currency {}, puede ser "CapabilityNeeded \n'.format(id_q, field, currency)
            logs[id_q]["fields"][field] = {"msg" : msg, "err": err}
    df = pd.concat(df, axis=1)
    logs[id_q]["Updated fields"] = len(df.columns)
    eq.update_values(df, keys)
    with open(dbpath + 'temp/save_update_state_load.pkl', 'wb') as file:
        pkl.dump(id_q, file)   

with open(dbpath + 'temp/update_logs.json', 'w') as file:
        json.dump(logs, file)     

program_end_time = time()
final_date = datetime.today()
sleep(1)

# Parse log information and send it via mail
html_parser = HtmlConverter(logs, daily_fields, quarter_fields, program_start_time, program_end_time, initial_date, final_date, companies)
mail_msg = html_parser.get_print()
mail_html = html_parser.get_html()
mailer = Mailer("[Acutalización tabla EquityMaster]", mail_msg, mail_html, "fpaniagua@larrainvial.com")
mailer.create_message("./files/temp/update_logs.json", "update_log.json")
mailer.send_message("fpaniagua@larrainvial.com")