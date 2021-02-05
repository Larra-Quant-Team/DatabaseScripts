import os
import pandas as pd
import pickle as pkl
import sys
from time import time, sleep
import json

from modules.apiciq.apicapitaliq import ApiCapitalIQ
from modules.tables import tables

dbpath = './files/'
fields = pd.read_excel(dbpath + 'Campos_SyP.xlsx')
fields = fields.loc[:, ['Campo_consulta', 'Periodicidad']]
quarter = fields.loc[fields['Periodicidad'] == 'Trimestral',
                     'Campo_consulta']
daily = fields.loc[fields['Periodicidad'] == 'Diaria',
                   'Campo_consulta']
daily_fields = daily.tolist()
quarter_fields = quarter.tolist()


# Get information about last update
last_update_info = {}
all_fields = quarter_fields + daily_fields
broken_mnemonics = ["IQ_EST_REV_DIFF_CIQ", "IQ_SPECIAL_DIV_SHARE", "IQ_INT_BEARING_DEPOSITS", "IQ_SUB_BONDS_NOTES",
                    "IQ_EST_EBITDA_DIFF_CIQ", "IQ_EST_EPS_DIFF_CIQ"]
quarter_fields = list(filter(lambda x: not x in broken_mnemonics, quarter_fields))   
daily_fields = list(filter(lambda x: not x in broken_mnemonics, daily_fields))       
initial_last_update_time = time()
for mnemonic in all_fields:    
    print(f"descagando información del ultimo update de {mnemonic}")
    if mnemonic in broken_mnemonics:
        continue
    eq = tables.EquityMaster(currency="Local", field=mnemonic)
    last_update_info[mnemonic] = eq.last_update()

with open(dbpath + 'last_update_info_dicc.pkl', 'wb') as file:
            pkl.dump(last_update_info, file)

print(f"tomo: {time()-initial_last_update_time}")