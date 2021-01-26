# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 17:37:18 2021.

@author: aback
"""
from datetime import datetime, timedelta
import os
import pandas as pd
import pickle as pkl
current_dir = os.getcwd()
os.chdir('./../ApiCiq')
from apicapitaliq import ApiCapitalIQ
os.chdir('./../Tables')
import tables
os.chdir(current_dir)

# Create Api object
api = ApiCapitalIQ()


# Load Companies
dbpath = 'C:/Users/aback/larrainvial.com/Equipo Quant - Documentos/Area Estrategias Cuantitativas 2.0/BDD/Reportafoleos/'
companies = pd.read_excel(dbpath + 'Company_Base_Definitivo.xlsx',
                          sheet_name='Compilado')
companies.set_index('ID_Quant', inplace=True)
companies.sort_index(inplace=True)
# Filter companies that aren't investable
companies = companies.loc[companies['Invertible'] == 1]

# TODO filter companies that are already in the database

# Load CIQ Fields

fields = pd.read_excel(dbpath + 'Campos_SyP.xlsx')
fields = fields.loc[:, ['Campo_consulta', 'Periodicidad']]
quarter = fields.loc[fields['Periodicidad'] == 'Trimestral',
                     'Campo_consulta']
daily = fields.loc[fields['Periodicidad'] == 'Diaria',
                   'Campo_consulta']
# daily_fields = {field: 'Valor' for field in daily}
# quarter_fields = {field: 'ValorTexto' if field == 'IQ_FILINGDATE_IS'
#                   else 'Valor' for field in quarter}
daily_fields = daily.tolist()
quarter_fields = quarter.tolist()

# Define request properties
# TODO modify start/end date for each company according to its current state
start = datetime(2010, 1, 1)
start_str = start.strftime('%Y-%m-%d')
end = (datetime.today() - timedelta(1))
end_str = end.strftime('%Y-%m-%d')
# TODO check properties are correct
properties_dusd = {'StartDate': start_str,
                   'EndDate': end_str,
                   'currencyid': 'USD'}
properties_qusd = {'periodtype': 'IQ_CQ-{}'.format(4 *
                                                   (end.year - start.year)),
                   'metadatatag': 'perioddate',
                   'currencyid': 'USD'}
properties_dloc = {'StartDate': start_str,
                   'EndDate': end_str,
                   'currencyid': 'Local'}
properties_qloc = {'periodtype': 'IQ_CQ-{}'.format(4 *
                                                   (end.year - start.year)),
                   'metadatatag': 'perioddate',
                   'currencyid': 'Local'}

# Load previous state, if existent
try:
    with open(dbpath + 'temp/save_state.pkl', 'rb') as file:
        last_i = pkl.load(file)
except FileNotFoundError:
    last_i = -1

# Download Data from CIQ for all companies
for i, isin in enumerate(companies['ISIN']):
    if i > last_i:
        if i % 25 == 0:
            print('{}. Consultando para {}'.format(i, isin))
        id_q = companies.loc[companies['ISIN'] == isin].index.item()
        # Create requests for each instrument
        requests = []
        # TODO handle FILING_DATE_IS request (doesn't have currency property)
        # requests.extend([api.historical_value(isin, mnemonic, properties_dusd)
        #                   for mnemonic in daily_fields])
        requests.extend([api.historical_value(isin, mnemonic, properties_qusd)
                          for mnemonic in quarter_fields])
        # requests.extend([api.historical_value(isin, mnemonic, properties_dloc)
        #                  for mnemonic in daily_fields])
        requests.extend([api.historical_value(isin, mnemonic, properties_qloc)
                          for mnemonic in quarter_fields])
        # Send request and save info
        response = api.sendRequest(requests)
        with open(dbpath + 'temp/historical_quarter_response_{}.pkl'.format(id_q),
                  'wb') as file:
            pkl.dump(response, file)
        # Save current state
        with open(dbpath + 'temp/save_state.pkl', 'wb') as file:
            pkl.dump(i, file)



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
    with open(dbpath + 'temp/save_state_load.pkl', 'rb') as file:
        last_id = pkl.load(file)
except FileNotFoundError:
    last_id = -1
logs = ''
for id_q, company in companies.iterrows():
    if id_q < last_id:
        continue
    if id_q % 25 == 0:
        print('Vamos en el id {}'.format(id_q))
    with open(dbpath + 'temp/historical_quarter_response_{}.pkl'.format(id_q),
              'rb') as file:
        response_q = pkl.load(file)
        if response_q.status_code != 200:
            logs += ('ID {} tiene status code {} para data quarter \n'
                    .format(id_q, response_q.status_code))
        response_q = response_q.json()
    with open(dbpath + 'temp/historical_response_{}.pkl'.format(id_q),
              'rb') as file:
        response_d = pkl.load(file)
        if response_d.status_code != 200:
            logs += ('ID {} tiene status code {} para data daily \n'
                    .format(id_q, response_d.status_code))
        response_d = response_d.json()
    df = []
    for res in [response_q, response_d]:
        for mnemo_data in res['GDSSDKResponse']:
            field = mnemo_data['Mnemonic']
            currency = mnemo_data['Properties']['currencyid']
            if mnemo_data['ErrMsg'] != '':
                logs += ('ID {} tiene error en campo {} con currency {}\n'
                         .format(id_q, field, currency))
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
                logs += 'ID {} tiene error en campo {} con currency {}, puede ser "CapabilityNeeded \n'.format(id_q, field, currency)
    df = pd.concat(df, axis=1)
    eq.insert(df, keys)
    with open(dbpath + 'temp/save_state_load.pkl', 'wb') as file:
        pkl.dump(id_q, file)
    with open(dbpath + 'temp/logs.txt', 'w') as file:
        file.write(logs)

    