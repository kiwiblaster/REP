#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 20:08:36 2024

@author: kevinfrank
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm

#---------------------------begin:data import and preperation for regression and calc CAR---------------------------------------------------

#import needed files
master = pd.read_excel('master.xlsx',index_col=0)
instruments = pd.read_excel('instruments_price.xlsx',index_col=0)
SSHI_index = pd.read_excel('SSHI_prices.xlsx',index_col=0)
SPX_index = pd.read_excel('SPX_prices.xlsx',index_col=0)
STOXX_index = pd.read_excel('STOXX_prices.xlsx',index_col=0)

#import fama french EUROPE and modify
fama_french_europe = pd.read_excel('Europe_5_Factors_Daily.xlsx')
fama_french_europe['Date'] = pd.to_datetime(fama_french_europe['Date'])
fama_french_europe = fama_french_europe[(fama_french_europe['Date'] >= '01-04-2010') & (fama_french_europe['Date'] <= '12-31-2023')]
fama_french_europe.set_index('Date', inplace=True)

#import fama french US and modify
fama_french_us = pd.read_excel('North_America_5_Factors_Daily.xlsx')
fama_french_us['Date'] = pd.to_datetime(fama_french_us['Date'])
fama_french_us = fama_french_us[(fama_french_us['Date'] >= '01-04-2010') & (fama_french_us['Date'] <= '12-31-2023')]
fama_french_us.set_index('Date', inplace=True)

#make SSHI instruments ready for calc
SSHI_instruments = master[(master['Index'] == 'SSHI')]
SSHI_instruments_unique = SSHI_instruments['RIC'].unique()
SSHI_instrument_prices = instruments.loc[:,SSHI_instruments_unique]
SSHI = pd.merge(SSHI_index, SSHI_instrument_prices, left_index=True, right_index=True, how='inner')
SSHI = pd.merge(SSHI, fama_french_europe, left_index=True, right_index=True, how='inner')
new_order = ['Mkt-RF','SMB','HML','RMW','CMA','RF']  
SSHI = SSHI[new_order + [col for col in SSHI.columns if col not in new_order]]
SSHI.to_excel('SSHI.xlsx', index=True)

#make SPX instruments ready for calc
SPX_instruments = master[(master['Index'] == 'SPX')]
SPX_instruments_unique = SPX_instruments['RIC'].unique()
#KeyError: "['CPAY.N'] not in index"
value_to_delete = 'CPAY.N'
index_to_delete = np.where(SPX_instruments_unique == value_to_delete)[0][0]
SPX_instruments_unique = np.delete(SPX_instruments_unique, index_to_delete, axis=0)
SPX_instrument_prices = instruments.loc[:,SPX_instruments_unique]
SPX = pd.merge(SPX_index, SPX_instrument_prices, left_index=True, right_index=True, how='inner')
SPX = pd.merge(SPX, fama_french_us, left_index=True, right_index=True, how='inner')
new_order = ['Mkt-RF','SMB','HML','RMW','CMA','RF']  
SPX = SPX[new_order + [col for col in SPX.columns if col not in new_order]]
SPX.to_excel('SPX.xlsx', index=True)


#make STOXX instruments ready for calc
STOXX_instruments = master[(master['Index'] == 'STOXX')]
STOXX_instruments_unique = STOXX_instruments['RIC'].unique()
STOXX_instrument_prices = instruments.loc[:,STOXX_instruments_unique]
STOXX = pd.merge(STOXX_index, STOXX_instrument_prices, left_index=True, right_index=True, how='inner')
STOXX = pd.merge(STOXX, fama_french_europe, left_index=True, right_index=True, how='inner')
new_order = ['Mkt-RF','SMB','HML','RMW','CMA','RF']  
STOXX = STOXX[new_order + [col for col in STOXX.columns if col not in new_order]]
STOXX.to_excel('STOXX.xlsx', index=True)

#---------------------------end:data import and preperation for regression and calc CAR---------------------------------------------------




#---------------------------begin:function for regression and calc CAR---------

def regression_and_calc_CAR(master_file,event_date,index_ric,constituent_data,days_before,days_after,index_data,fama_french,car_event1,car_event_lower,car_event_upper):
    

    
    master_file.reset_index(inplace=True)
    event = master_file[(master_file['Date'] == event_date) & (master_file['Index'] == index_ric)]   #
    event_ric = event['RIC']
    price_event = constituent_data.loc[:, event_ric]
    
   #define date range
    target_date = pd.to_datetime(event_date)
    target_date_index = price_event.index.get_loc(target_date)
    
    start_date = target_date_index - days_before
    end_date = target_date_index + days_after
        
    filtered_df = price_event.iloc[start_date:end_date]
    filtered_df = filtered_df.fillna(method='ffill').fillna(method='bfill')
   
    filtered_dataframe = pd.merge(filtered_df, index_data, left_index=True, right_index=True, how='inner')
    log_returns_event = np.log(filtered_dataframe / filtered_dataframe.shift(1))
    log_returns_event = log_returns_event.drop(log_returns_event.index[0])
    log_returns_event = pd.merge(log_returns_event, fama_french, left_index=True, right_index=True, how='inner')
    new_order = [index_ric,'Mkt-RF','SMB','HML','RMW','CMA','RF']  
    log_returns_event = log_returns_event[new_order + [col for col in log_returns_event.columns if col not in new_order]]
    
    log_returns_event.to_excel(index_ric + event_date + "_log.xlsx")
    
    final_SAR = pd.DataFrame(index=log_returns_event.index)

    
    drop = [index_ric,'Mkt-RF','SMB','HML','RMW','CMA','RF']
    instruments = [col for col in log_returns_event.columns if col not in drop]
    
    coefficients_df = pd.DataFrame(columns=['RIC','Date','Rm-Rf', 'SMB', 'HML', 'RMW', 'CMA'])
  
    for ric in instruments:
        
        instrument = log_returns_event[[ric,index_ric,'Mkt-RF','SMB','HML','RMW','CMA','RF']]
        
        index_df = pd.DataFrame(index=instrument.index)
        regression = instrument.assign(Y=instrument[ric] - instrument['RF'], X1=instrument[index_ric] - instrument['RF'])
        
        index_event_date = regression.index.get_loc(event_date)
        
        regression_date = index_event_date - 1
                
        final_date = regression.index[regression_date]
        
        datum_string = final_date.strftime('%Y-%m-%d')
        
        regression = regression.loc[regression.index <= datum_string]
        
        X = regression[['X1','SMB','HML','RMW','CMA']]
        Y = regression['Y']
        
        #X = sm.add_constant(X)
        model = sm.OLS(Y, X).fit(intercept=False)
        
        coefficients = model.params
        
        
        
        coefficients_df = coefficients_df.append({
            'RIC': ric,
            'Date': event_date,
            'Rm-Rf': coefficients[0],
            'SMB': coefficients[1],
            'HML': coefficients[2],
            'RMW': coefficients[3],
            'CMA': coefficients[4]
        }, ignore_index=True)
        

        index_df['Rm-Rf'] = coefficients[0]
        index_df['SMB'] = coefficients[1]
        index_df['HML'] = coefficients[2]
        index_df['RMW'] = coefficients[3]
        index_df['CMA'] = coefficients[4]
        index_df['Rit'] = instrument[ric]
        index_df['Expected Rit-Rf'] = index_df['Rm-Rf'] * (instrument[index_ric]-instrument['RF'])+ index_df['SMB'] * instrument['SMB'] + index_df['HML'] * instrument['HML'] + index_df['RMW'] * instrument['RMW'] + index_df['CMA'] * instrument['CMA']
        index_df['AR'] = index_df['Rit']-index_df['Expected Rit-Rf']
        index_df['SAR'] = index_df['AR']/np.std(index_df['AR'])
        final_SAR = pd.merge(final_SAR, index_df[['SAR']], left_index=True, right_index=True, how='left')
        final_SAR.rename(columns={'SAR': ric}, inplace=True)
        
    
    final_SAR.to_excel(index_ric + event_date + "_SAR.xlsx")
    coefficients_df.to_excel(index_ric + event_date + "_regression.xlsx",index=False)

    
    df = pd.DataFrame(final_SAR[car_event_lower:car_event_upper])
    cumulative_sum = df.cumsum()
    CAR = cumulative_sum.tail(1)
    
    new_index = [car_event1]

    # Index neu setzen und alten Index löschen
    CAR.set_index(pd.Index(new_index), inplace=True)
            
    return CAR
    

#---------------------------end:function for regression and calc CAR-----------
#testing one event   
final_CAR = regression_and_calc_CAR(master_file=master.copy(),
                       event_date='2011-10-20',
                       index_ric='SSHI',
                       constituent_data=SSHI,
                       days_before=101,
                       days_after=36,
                       index_data=SSHI_index,
                       fama_french=fama_french_europe,
                       car_event1='(3,3)', car_event_lower=97,car_event_upper=103)  


#---------------------------begin:call function regression_and_calc_CAR--------

from datetime import datetime

# Liste der Datumsangaben im europäischen Format
#PROBLEM '15.04.2010','2018-03-22'
dates_swiss = [
    "15.04.2010", "17.12.2010", "11.03.2011", "20.10.2011", "24.04.2013", 
    "19.03.2014", "15.01.2015", "13.11.2015", "24.06.2016", "09.11.2016", 
    "22.03.2018", "09.05.2018", "23.03.2018", "29.10.2018", "15.02.2019", 
    "24.05.2019", "23.01.2020", "21.02.2020", "30.06.2020", "09.11.2020", 
    "01.02.2021", "23.03.2021", "15.04.2021", "24.02.2022", "17.03.2022", 
    "01.12.2022", "06.02.2023"
]

# Umwandlung der Datumsangaben ins amerikanische Format und Speicherung in einer neuen Liste
dates_switzerland = [datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d") for date in dates_swiss]
event_dates_switzerland = ['2010-12-17', '2011-03-11', '2011-10-20', '2013-04-24', '2014-03-19', '2015-01-15', '2015-11-13','2016-06-24', '2016-11-09','2018-05-09', '2018-03-23', '2018-10-29', '2019-02-15', '2019-05-24', '2020-01-23', '2020-02-21', '2020-06-30', '2020-11-09', '2021-02-01', '2021-03-23', '2021-04-15', '2022-02-24', '2022-03-17', '2022-12-01', '2023-02-06']

dates_eu = [
    "15.04.2010", "17.12.2010", "11.03.2011", "20.10.2011", "24.04.2013", 
    "19.03.2014", "15.01.2015", "13.11.2015", "24.06.2016", "09.11.2016", 
    "22.03.2018", "09.05.2018", "23.03.2018", "29.10.2018", "15.02.2019", 
    "24.05.2019", "23.01.2020", "21.02.2020", "30.06.2020", "09.11.2020", 
    "01.02.2021", "23.03.2021", "15.04.2021", "24.02.2022", "17.03.2022", 
    "01.12.2022", "06.02.2023"
]

# Umwandlung der Datumsangaben ins amerikanische Format und Speicherung in einer neuen Liste
dates_eu = [datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d") for date in dates_eu]
event_dates_eu = ['2010-12-17', '2011-03-11', '2011-10-20', '2013-04-24', '2014-03-19', '2015-01-15', '2015-11-13','2016-06-24', '2016-11-09','2018-05-09', '2018-03-23', '2018-10-29', '2019-02-15', '2019-05-24', '2020-01-23', '2020-02-21', '2020-06-30', '2020-11-09', '2021-02-01', '2021-03-23', '2021-04-15', '2022-02-24', '2022-03-17', '2022-12-01', '2023-02-06']

dates_us = [
    "15.04.2010","17.12.2010","11.03.2011","20.10.2011","24.04.2013",
    "18.03.2014","15.01.2015","16.11.2015","24.06.2016","09.11.2016",
    "22.03.2018","08.05.2018","23.03.2018","29.10.2018","15.02.2019",
    "24.05.2019","23.01.2020","21.02.2020","30.06.2020","09.11.2020",
    "01.02.2021","23.03.2021","15.04.2021","24.02.2022","17.03.2022",
    "30.11.2022","06.02.2023"]

# Umwandlung der Datumsangaben ins amerikanische Format und Speicherung in einer neuen Liste
dates_us = [datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d") for date in dates_us]
event_dates_us = ['2010-12-17', '2011-03-11', '2011-10-20', '2013-04-24', '2014-03-18', '2015-01-15', '2015-11-16', '2016-06-24', '2016-11-09','2018-05-08', '2018-03-23', '2018-10-29', '2019-02-15', '2019-05-24', '2020-01-23', '2020-02-21', '2020-06-30', '2020-11-09', '2021-02-01', '2021-03-23', '2021-04-15', '2022-02-24', '2022-03-17', '2022-11-30', '2023-02-06']




def call_function(INDEX,event_dates,constituent_data,fama_french,index_data):

    # Verschiedene Werte für die Parameter car_event1, car_event_lower und car_event_upper
    car_events = [
        {'car_event1': '(-3,3)', 'car_event_lower': 97, 'car_event_upper': 103},
        {'car_event1': '(-5,5)', 'car_event_lower': 95, 'car_event_upper': 105},
        {'car_event1': '(-2,5)', 'car_event_lower': 98, 'car_event_upper': 105},
        {'car_event1': '(-5,2)', 'car_event_lower': 95, 'car_event_upper': 102},
        {'car_event1': '(0,10)', 'car_event_lower': 100, 'car_event_upper': 110}
    ]
    
    # Mehrere Event-Daten
    #event_dates = ['2011-03-11', '2015-01-15']
    
    
    
    # Initialisierung des DataFrames außerhalb der Schleifen
    result_df = pd.DataFrame(columns=['Instrument', 'EventDate', 'CAR_Type', 'CAR'])
    
    # Schleife über die verschiedenen Event-Daten
    for event_date in event_dates:
        for car_event in car_events:
            car_event_val = car_event['car_event1']
    
            # Debug: Ausgabe des aktuellen Datums und Ereignisses
            #print(f"Running regression for event date {event_date} and car_event {car_event_val}")
    
            # Ausführung der regression Funktion
            final_CAR = regression_and_calc_CAR(master_file=master.copy(),
                                   event_date=event_date,
                                   index_ric=INDEX,
                                   constituent_data=constituent_data,
                                   days_before=101,
                                   days_after=36,
                                   index_data=index_data,
                                   fama_french=fama_french,
                                   **car_event)
    
            # Debug: Ausgabe des erhaltenen DataFrame
            #print(f"Data received for {event_date} {car_event_val}:\n{final_CAR}")
    
            # Annahme: final_CAR ist ein DataFrame
            reshaped_df = final_CAR.melt(var_name='Instrument', value_name='CAR')
            reshaped_df['EventDate'] = event_date
            reshaped_df['CAR_Type'] = car_event_val
            result_df = pd.concat([result_df, reshaped_df], ignore_index=True)
    
    return result_df
    

CAR_SSHI = call_function(INDEX='SSHI',event_dates=event_dates_switzerland,constituent_data=SSHI,fama_french=fama_french_europe,index_data=SSHI_index)
CAR_STOXX = call_function(INDEX='STOXX',event_dates=event_dates_eu,constituent_data=STOXX,fama_french=fama_french_europe,index_data=STOXX_index)
CAR_SPX = call_function(INDEX='SPX',event_dates=event_dates_us,constituent_data=SPX,fama_french=fama_french_us,index_data=SPX_index)


CAR_SSHI.to_excel('CAR_SSHI.xlsx',index=False)
CAR_STOXX.to_excel('CAR_STOXX.xlsx',index=False)
CAR_SPX.to_excel('CAR_SPX.xlsx',index=False)



