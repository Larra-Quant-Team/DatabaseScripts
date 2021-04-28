from datetime import datetime, timedelta
import os
import pandas as pd
import pickle as pkl
import sys
from time import time, sleep
import json
import matplotlib.pyplot as plt
import numpy as np
# local moduls
from modules.apiciq.apicapitaliq import ApiCapitalIQ
from modules.tables import tables

def create_key(region, currency, field, source="QUANT/CIQ", instrument="INDEX"):
    country = region
    currency = currency
    instrument = instrument
    source = source
    field = field
    return '.'.join([country, currency, instrument, source, field])


# median IQ_VALUE_TRADED in the last n days
def calculate_adtv(value_traded_df, n=90):
    return value_traded_df.rolling(n).median(skipna=True)
    #roller = value_traded_df.rolling(n)
    #adtv = roller.median(skipna=True)
    #adtv = adtv.iloc[n+1:]
    #adtv = adtv.dropna()
    #return adtv

def weight_market(assets, asset_returns, asset_marketcaps):
    weights = asset_marketcaps.divide(asset_marketcaps.sum(1), axis=0)
    return asset_returns (asset_returns * weights.shift(1)).sum(1)
    #all_marketcap = sum(asset_marketcaps.values.tolist())
    #weighted_return = sum([asset_returns[id_q].iloc[0].item()*asset_marketcaps[id_q] for id_q in assets])
    #return weighted_return/all_marketcap

def calculate_global_index(local_index, countries_market_caps):
    print("calculating global index")
    new_serie_info = {}
    for date, row in local_index.iterrows():
        row = row.dropna()
        index_keys = row.index.values.tolist()
        #print(date)
        #print([countries_market_caps[key.split(".")[0]][date.strftime('%Y-%m-%d')] for key in index_keys])
        all_marketcap = sum([countries_market_caps[key.split(".")[0]][date.strftime('%Y-%m-%d')] for key in index_keys]) 
        weighted_return = sum([row[key]*countries_market_caps[key.split(".")[0]][date.strftime('%Y-%m-%d')] for key in index_keys])
        if all_marketcap:
            new_serie_info[date] = weighted_return/all_marketcap
    index_serie = pd.Series(new_serie_info)
    index_serie.name = create_key("GLOBAL", "USD", "WEIGHTED_RETURN") 
    return index_serie    

def calculate_index(country, adtv_treshold = 0.5):      
    eq = tables.EquityMaster(country=country,  currency="USD", field=["IQ_VALUE_TRADED"])
    query = eq.query(rename="asset")
    adtv = calculate_adtv(query.fillna(0), 90)
    #print(query.fillna(0))
    #print(adtv)
    serie_info = {}
    market_cap_info = {}
    previus_date = None
    eq_asset_price = tables.EquityMaster(country=country, currency="USD", 
                                            field=["IQ_CLOSEPRICE"])
    eq_asset_marketcap = tables.EquityMaster(country=country, currency="USD", 
                                            field=["IQ_MARKETCAP"])  
    asset_prices = eq_asset_price.query(rename=["asset"])
    asset_marketcaps = eq_asset_marketcap.query(rename=["asset"])      
    start_time = time()                                    
    for date, row in adtv.iterrows():       
        if not previus_date:
            previus_date = date.to_pydatetime()
            continue
        filter_assets = filter(lambda asset_adtv : asset_adtv[1] >= adtv_treshold , row.to_dict().items())
        filter_assets = list(map(lambda asset_adt: asset_adt[0], filter_assets))
        end_date = date.to_pydatetime()
        start_date = previus_date
        #print(f"calculating index for {country} at {date} with #{len(filter_assets)} assets")
        #print(f"previus date: {start_date}")
                                                 
        asset_marketcap = asset_marketcaps.loc[end_date.strftime('%Y-%m-%d')].fillna(0)     
        asset_price = asset_prices[start_date.strftime('%Y-%m-%d'):end_date.strftime('%Y-%m-%d')]                           
        contain_filter_assets = set(filter_assets) & set(asset_price.columns.values.tolist()) & set(asset_marketcap.index.values.tolist())
        contain_filter_assets = list(contain_filter_assets)
        #asset_price = asset_price[contain_filter_assets]
        #asset_marketcap = asset_marketcap[contain_filter_assets]                            
        asset_returns = asset_price.pct_change().tail(1)
        '''
        print(asset_price)
        print("---")
        print(asset_returns)
        print("---")
        print(asset_marketcap)
        '''
        weighted_return = weigh_market(contain_filter_assets, asset_returns, asset_marketcap)
        serie_info[end_date] = weighted_return 
        market_cap_info[end_date.strftime('%Y-%m-%d')] = sum(asset_marketcap.values.tolist()) 
        previus_date = date
    print(f"time: {time() - start_time}")
    index_serie = pd.Series(serie_info)
    index_serie.name = create_key(country, "USD", "WEIGHTED_RETURN") 
    return index_serie, market_cap_info

if __name__ == "__main__":
    COUNTRIES_TRESHOLDS = {"Chile": 0.5, "Brazil": 1, "Colombia": 0.5, "Mexico": 0.5,
                           "Peru": 0.5, "Argentina": 0.5}
    df = []
    countries_market_caps = {}
    for country_name, country_treshold in COUNTRIES_TRESHOLDS.items():
        print(f"calculatung index for {country_name} with {country_treshold} ADTV treshold")
        country_quant_index, market_cap_info = calculate_index(country_name, country_treshold)
        df.append(country_quant_index)
        countries_market_caps[country_name] = market_cap_info
    locals_df = pd.concat(df, axis=1)
    all_market = calculate_global_index(locals_df, countries_market_caps)
    df.append(all_market)
    df = pd.concat(df, axis=1)
    #print(df)
    eq = tables.MacroMaster()
    keys = eq.get_keys()
    eq.insert(df, keys)    
    
