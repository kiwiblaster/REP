#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 20:08:36 2024

@author: kevinfrank
"""

import pandas as pd

master = pd.read_excel('master.xlsx')
price = pd.read_excel('instruments_price.xlsx')
price = price.set_index('Date')

event1 = master[(master['Date'] == '2010-04-15') & (master['Index'] == 'SSHI')]
event1_ric = event1['RIC']

price_event1 = price.loc[:, event1_ric]


def filter_dataframe_by_date_range(df, target_date,days_before,days_after):
    
    target_date = pd.to_datetime(target_date)
    start_date = target_date - pd.Timedelta(days=days_before)
    end_date = target_date + pd.Timedelta(days=days_after)
    filtered_df = df.loc[start_date:end_date]
    filtered_df = filtered_df.fillna(method='ffill').fillna(method='bfill')
    return filtered_df


filtered_dataframe = filter_dataframe_by_date_range(price_event1, '2010-04-15',30,30)
