# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 13:06:47 2019

@author: Aback
"""

from apicapitaliq import ApiCapitalIQ
import pyodbc
import pandas as pd
import numpy as np
import json
import time


def get_companies(dynamo):
    comps = dynamo.scan_table('company_base')                                          })
    companies = pd.DataFrame(comps['Items'])
    companies = companies.loc[companies['Invertible_index'] == 1]
    return companies

def get_daily_headers(cnxn):
    query = "SELECT TOP 1 * FROM [LARRA_DOM\\Moksenberg].[Daily_Data]"
    return pd.read_sql_query(query, cnxn).columns.tolist()


def get_quarter_headers(cnxn):
    query = "SELECT TOP 1 * FROM [LARRA_DOM\\Moksenberg].[Quarter_Data]"
    return pd.read_sql_query(query, cnxn).columns.tolist()


if __name__ == '__main__':

    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=LAJA;DATABASE=BDGeneral')
    companies = get_companies(cnxn)
    daily_headers = list(filter(lambda x: 'IQ' in x, get_daily_headers(cnxn)))
    quarter_headers = list(filter(lambda x: 'IQ' in x,
                                  get_quarter_headers(cnxn)))

    # Get username, password and applicationid
    base_url = 'https://api-ciq.marketintelligence.spglobal.com/gdsapi/rest/v3/clientservice.json'
    username = 'apiadmin1@larrainvial.com'
    password = 'LarrainVial.2020'

    api = ApiCapitalIQ(base_url, username, password)

    requests = []
    for isin in companies['ISIN']:

        requests.extend([api.historical_value(isin, mnemonic) for mnemonic
                         in daily_headers])

    response = []
    start = time.time()
    response.extend(api.sendRequest(requests[x:x + 100]) for x in range(0,
                    len(requests), 100))
    print("Requests took {} seconds".format(time.time() - start))
    # with open('capitaliqfundamentals.json', 'w') as file:
    #     json.dump(response, file)
        
        
    # Dataframe with CapitalIq info
    capitaliq_df = pd.DataFrame()
    for _id in response.keys():   
        for mnemonic_ans in response[_id]['GDSSDKResponse']:
            mnemonic = mnemonic_ans['Headers'][0]
            row = mnemonic_ans['Rows'][0]['Row'][0]
            capitaliq_df.loc[_id, mnemonic] = row
    capitaliq_df[capitaliq_df == 'Data Unavailable'] = np.nan
    capitaliq_df[capitaliq_df == 'NaN'] = np.nan

    for col in capitaliq_df.columns:
        if col != 'IQ_FILINGDATE_IS':
            capitaliq_df[col] = capitaliq_df[col].astype(float)



