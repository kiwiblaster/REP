#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 08:44:31 2023

@author: kevinfrank
"""

"Test"

import pandas as pd
import refinitiv.data as rd
import time
import os as os


def indexconst_and_data(index,fields, start, end, interval):
    data_list = []
    rd.open_session()
    const_indices = rd.get_data(universe=index, fields=fields)
    
    ric_list = const_indices['Instrument']
   
    data_list = []
    
    for index, ric in enumerate(ric_list):
        while True:
            try:
                rd.open_session()
                data = rd.get_history(universe=ric, fields='TR.CLOSEPRICE', interval=interval, start=start, end=end)
                rd.close_session()
                data_list.append(data)
                data.columns = [ric]
                print(ric)
                print(len(ric_list) - index)
                break
            except Exception as e:
                print(f"Fehler beim Abrufen von {ric}: {str(e)}")
                time.sleep(1)
 
    
    historical_data_df = pd.concat(data_list, axis=1)
    
    return const_indices,historical_data_df
    
    
const_DJA_1995, data_DJA_1995  = indexconst_and_data(index=['0#.DJA(1995-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='1995-1-1',end='1995-12-31',interval='daily')


rd.open_session()
SPX_daily = rd.get_history(universe=".SPX", fields='TR.PriceCLose', interval='daily', start='1998-1-31', end='2023-08-31')
SPX_monthly = rd.get_history(universe=".SPX", fields='TR.PriceCLose', interval='monthly', start='1978-2-28', end='2023-09-30')

DJA_daily = rd.get_history(universe=".DJA", fields='TR.PriceCLose', interval='daily', start='1998-1-31', end='2023-08-31')
DJA_monthly = rd.get_history(universe=".DJA", fields='TR.PriceCLose', interval='monthly', start='1998-1-31', end='2023-08-31')

rd.open_session()
const_SPX_2007 = rd.get_data(universe='0#.SPX(2007-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])

const_SPX_2007 = const_SPX_2007[const_SPX_2007['GICS Sector Name'] != '']


def create_sector_equities_df(df):
   
    dummy_df = pd.get_dummies(df['GICS Sector Name'])
    dummy_df.insert(0, 'Instrument', df['Instrument'])
    result_df = dummy_df.groupby('Instrument').sum()
    result_df[result_df > 1] = 1

    return result_df

sector_equities_2007 = create_sector_equities_df(const_SPX_2007)


def final_datasets(df, const, sector, year):
    valid_tickers = const['Instrument']
    df = df[valid_tickers]
    #df = df.drop(df.index[-1])
    df = df.drop(df.index[0])
    df = df.dropna(axis=1)
    print(df.isna().sum().sum())
    valid_tickers = df.columns
    selected_variables = valid_tickers.values
    selected_columns = sector.loc[selected_variables]
    
    
    if df.shape[1] != selected_columns.shape[0]:
        print("Error: Not the same Dimension")
    else:
        print("Well done!")

    
    df.to_excel(f"data_SPX_{year}.xlsx", index=True)
    selected_columns.to_excel(f"const_SPX_{year}.xlsx", index=True)

    return df, selected_columns


def export_dataframes_to_excel(string):
    global_vars = globals()

    
    dataframes = {var_name: var_value for var_name, var_value in global_vars.items() if isinstance(var_value, pd.DataFrame) and var_name.startswith(string)}

    
    for df_name, df in dataframes.items():
        file_name = f"{df_name}.xlsx"
        df.to_excel(file_name, index=True)
        print(f'DataFrame "{df_name}" wurde in {file_name} exportiert.')


export_dataframes_to_excel("data")



def indexconst_and_data(ric,fields, start, end, interval):
    
    ric_list = ric
    data_list = []
    
    for index, ric in enumerate(ric_list):
        while True:
            try:
                rd.open_session()
                data = rd.get_history(universe=ric, fields='TR.CLOSEPRICE', interval=interval, start=start, end=end)
                rd.close_session()
                data_list.append(data)
                data.columns = [ric]
                print(ric)
                print(len(ric_list) - index)
                break
            except Exception as e:
                print(f"Fehler beim Abrufen von {ric}: {str(e)}")
                time.sleep(1)
 
    
    historical_data_df = pd.concat(data_list, axis=1)
    
    return historical_data_df

