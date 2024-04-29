#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 18:08:14 2024

@author: kevinfrank
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 20:08:36 2024

@author: kevinfrank
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
import warnings
warnings.filterwarnings('ignore')
from scipy.stats import binom_test


#---------------------------begin:data import and preperation for regression and calc CAR---------------------------------------------------

#import needed files
master = pd.read_excel('master.xlsx',index_col=0)
#master['Date'] = pd.to_datetime(master['Date'])
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
#SSHI.to_excel('SSHI.xlsx', index=True)

#make SPX instruments ready for calc
SPX_instruments = master[(master['Index'] == 'SPX')]
SPX_instruments_unique = SPX_instruments['RIC'].unique()


#KeyError: "['CPAY.N'] not in index"
#value_to_delete = 'CPAY.N'
#index_to_delete = np.where(SPX_instruments_unique == value_to_delete)[0][0]
#SPX_instruments_unique = np.delete(SPX_instruments_unique, index_to_delete, axis=0)


SPX_instrument_prices = instruments.loc[:,SPX_instruments_unique]
SPX = pd.merge(SPX_index, SPX_instrument_prices, left_index=True, right_index=True, how='inner')
SPX = pd.merge(SPX, fama_french_us, left_index=True, right_index=True, how='inner')
new_order = ['Mkt-RF','SMB','HML','RMW','CMA','RF']  
SPX = SPX[new_order + [col for col in SPX.columns if col not in new_order]]
#SPX.to_excel('SPX.xlsx', index=True)


#make STOXX instruments ready for calc
STOXX_instruments = master[(master['Index'] == 'STOXX')]
STOXX_instruments_unique = STOXX_instruments['RIC'].unique()
STOXX_instrument_prices = instruments.loc[:,STOXX_instruments_unique]
STOXX = pd.merge(STOXX_index, STOXX_instrument_prices, left_index=True, right_index=True, how='inner')
STOXX = pd.merge(STOXX, fama_french_europe, left_index=True, right_index=True, how='inner')
new_order = ['Mkt-RF','SMB','HML','RMW','CMA','RF']  
STOXX = STOXX[new_order + [col for col in STOXX.columns if col not in new_order]]
#STOXX.to_excel('STOXX.xlsx', index=True)

#---------------------------end:data import and preperation for regression and calc CAR---------------------------------------------------




#---------------------------begin:function for regression and calc CAR---------

def regression_and_calc(master_file,event_date,index_ric,constituent_data,days_before,days_after,index_data,fama_french,car_event1,sector):
    

    
    master_file.reset_index(inplace=True)
    event = master_file[(master_file['Date'] == event_date) & (master_file['Index'] == index_ric) & (master_file['TRBC Economic Sector Name'] == sector)] #
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
    
    #log_returns_event.to_excel(index_ric + event_date + "_log.xlsx")
    
    final_SAR = pd.DataFrame(index=log_returns_event.index)
    final_AR =  pd.DataFrame(index=log_returns_event.index)

    
    drop = [index_ric,'Mkt-RF','SMB','HML','RMW','CMA','RF']
    instruments = [col for col in log_returns_event.columns if col not in drop]
    
    coefficients_df = pd.DataFrame(columns=['RIC','Date','Rm-Rf', 'SMB', 'HML', 'RMW', 'CMA'])
  
    values_grank = pd.DataFrame(columns=['RIC','Event_Date','Event_Window','CAR','VAR_AR_Event','VAR_CAR_Event','STD_CAR_Event','SCAR'])
    
    
    
    TGRANK = pd.DataFrame(columns=['Event_Date','Event_Window','Sector','SU','Z','TGRANK'])
    SIGN_TEST = pd.DataFrame(columns=['Event_Date','Event_Window','Sector','Z_SIGN'])
    GENERALIZED_SIGN_TEST = pd.DataFrame(columns=['Event_Date','Event_Window','Sector','W','P_HAT','Z_GS_SIGN'])
    TEST_STATISTICS = pd.DataFrame(columns=['Event_Date','Event_Window','Sector','T_GRANK','Z_GSIGN','Z_SIGN'])

    
  
    for ric in instruments:
        
        instrument = log_returns_event[[ric,index_ric,'Mkt-RF','SMB','HML','RMW','CMA','RF']]
        
        index_df = pd.DataFrame(index=instrument.index)
        regression = instrument.assign(Y=instrument[ric] - instrument['RF'], X1=instrument[index_ric] - instrument['RF'])
        
        
        regression = regression.iloc[:100]
        
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
        final_AR = pd.merge(final_AR, index_df[['AR']], left_index=True, right_index=True, how='left')
        final_AR.rename(columns={'AR': ric}, inplace=True)
       

        index_df['SAR'] = index_df['AR']/np.std(index_df['AR'][:100])
        
        
        
        final_SAR = pd.merge(final_SAR, index_df[['SAR']], left_index=True, right_index=True, how='left')
        final_SAR.rename(columns={'SAR': ric}, inplace=True)
        
        
        values_grank = values_grank.append({
            'RIC': ric,
            'Event_Date': event_date,
            'Event_Window': car_event1,
            'Sector': sector,
            'CAR': index_df['AR'][100:].sum(),
            'VAR_AR_Event': index_df['AR'][100:].var(ddof=2),
            'VAR_CAR_Event': index_df['AR'][100:].var(ddof=2) * len(index_df[100:]),
            'STD_CAR_Event': np.sqrt(index_df['AR'][100:].var(ddof=2) * len(index_df[100:])),
            'SCAR': index_df['AR'][100:].sum() / np.sqrt(index_df['AR'][100:].var(ddof=2) * len(index_df[100:]))
        }, ignore_index=True)
        
        
    SIGN_TEST = SIGN_TEST.append({
        'Event_Date': event_date,
        'Event_Window': car_event1,
        'Sector':sector,
        'Z_SIGN': ((values_grank['CAR']>0).sum() / len(values_grank['CAR'])-0.5) * (np.sqrt(len(values_grank['CAR']))/0.5)
    
    }, ignore_index=True)
    
    
    GENERALIZED_SIGN_TEST = GENERALIZED_SIGN_TEST.append({
        'Event_Date': event_date,
        'Event_Window': car_event1,
        'Sector': sector,
        'W': (values_grank['CAR']>0).sum(),
        'P_HAT': 1/len(final_AR.columns)*1/len(final_AR)*(final_AR>0).sum().sum(),
        'Z_GS_SIGN': ((values_grank['CAR']>0).sum()-len(final_AR.columns)*1/len(final_AR.columns)*1/len(final_AR)*(final_AR>0).sum().sum())\
            /np.sqrt(len(final_AR.columns)*1/len(final_AR.columns)*1/len(final_AR)*(final_AR>0).sum().sum()*(1-1/len(final_AR.columns)*1/len(final_AR)*(final_AR>0).sum().sum()))
    
    }, ignore_index=True)
        

    
    values_grank['STD_SCAR'] = values_grank['SCAR'].std(ddof=1)
    values_grank['SCAR*'] = values_grank['SCAR'] / values_grank['STD_SCAR']
    
    GSAR = final_SAR.iloc[:100,:]
    
    # Wähle die 8. Spalte des dritten DataFrame
    column = values_grank.iloc[:, 9]
    length = len(GSAR)
    x=0
    # Füge die letzten Werte jeder Spalte des dritten DataFrames zu neues_df hinzu
    for spalte in GSAR:
        
        letzter_wert = column.iloc[x]
        x += 1
        GSAR.at[length, spalte] = letzter_wert
    
    rank = GSAR.rank(ascending=True)
    
    DSAR = (rank / (101+1)) - 0.5 #Demeaned Standardized Abnormal Ranks
    
    DSAR['UT_DACH_HOCH2'] =(DSAR.sum(axis=1) / len(DSAR.columns))**2
    
    TGRANK = TGRANK.append({
        'Event_Date': event_date,
        'Event_Window': car_event1,
        'Sector': sector,
        'SU': np.sqrt(DSAR['UT_DACH_HOCH2'].sum()/101),
        'Z': DSAR.iloc[-1].mean() / np.sqrt(DSAR['UT_DACH_HOCH2'].sum()/101),
        'TGRANK': (DSAR.iloc[-1].mean() / np.sqrt(DSAR['UT_DACH_HOCH2'].sum()/101))*np.sqrt((101-2)/(101-1-(DSAR.iloc[-1].mean() / np.sqrt(DSAR['UT_DACH_HOCH2'].sum()/101))**2))
    }, ignore_index=True)
    
    TEST_STATISTICS = TEST_STATISTICS.append({
        'Event_Date': event_date,
        'Event_Window': car_event1,
        'Sector': sector,
        'T_GRANK': (DSAR.iloc[-1].mean() / np.sqrt(DSAR['UT_DACH_HOCH2'].sum()/101))*np.sqrt((101-2)/(101-1-(DSAR.iloc[-1].mean() / np.sqrt(DSAR['UT_DACH_HOCH2'].sum()/101))**2)),
        'Z_GSIGN': ((values_grank['CAR']>0).sum()-len(final_AR.columns)*1/len(final_AR.columns)*1/len(final_AR)*(final_AR>0).sum().sum())\
            /np.sqrt(len(final_AR.columns)*1/len(final_AR.columns)*1/len(final_AR)*(final_AR>0).sum().sum()*(1-1/len(final_AR.columns)*1/len(final_AR)*(final_AR>0).sum().sum())),
        'Z_SIGN': ((values_grank['CAR']>0).sum() / len(values_grank['CAR'])-0.5) * (np.sqrt(len(values_grank['CAR']))/0.5)
    }, ignore_index=True)  
    
    
    
    with pd.ExcelWriter(index_ric + "_" + event_date + "_" + car_event1 + "_" + sector + ".xlsx") as writer:  
        log_returns_event.to_excel(writer, sheet_name='Log_Returns_and_Fama_French')
        coefficients_df.to_excel(writer, sheet_name='Coefficients_Regression')
        final_AR.to_excel(writer, sheet_name='AR')
        final_SAR.to_excel(writer, sheet_name='SAR')
        rank.to_excel(writer, sheet_name='RANK')
        GSAR.to_excel(writer, sheet_name='GSAR')
        values_grank.to_excel(writer, sheet_name='values_grank')
        DSAR.to_excel(writer, sheet_name='DSAR')



    return TGRANK,SIGN_TEST,GENERALIZED_SIGN_TEST,TEST_STATISTICS
        

#---------------------------end:function for regression and calc CAR-----------

        



#---------------------------begin:call function regression_and_calc--------


event_dates_switzerland = ['2010-12-17',
 '2011-03-11',
 '2011-10-20',
 '2011-11-07',
 '2013-04-24',
 '2014-03-19',
 '2015-01-15',
 '2015-11-16',
 '2016-06-24',
 '2016-11-09',
 '2018-05-09',
 '2018-10-29',
 '2019-03-15',
 '2019-05-24',
 '2020-01-23',
 '2020-02-21',
 '2020-06-30',
 '2020-11-09',
 '2021-02-01',
 '2021-03-23',
 '2021-04-15',
 '2022-02-24',
 '2022-03-17',
 '2022-09-19',
 '2022-12-01',
 '2023-02-06',
 '2023-04-17',
 '2023-10-09',
 '2023-11-20']




event_dates_eu = ['2010-12-17',
 '2011-03-11',
 '2011-10-20',
 '2011-11-07',
 '2013-04-24',
 '2014-03-19',
 '2015-01-15',
 '2015-11-16',
 '2016-06-24',
 '2016-11-09',
 '2018-05-09',
 '2018-10-29',
 '2019-03-15',
 '2019-05-24',
 '2020-01-23',
 '2020-02-21',
 '2020-06-30',
 '2020-11-09',
 '2021-02-01',
 '2021-03-23',
 '2021-04-15',
 '2022-02-24',
 '2022-03-17',
 '2022-09-19',
 '2022-12-01',
 '2023-02-06',
 '2023-04-17',
 '2023-10-09',
 '2023-11-20']


event_dates_us = ['2010-12-17',
 '2011-03-11',
 '2011-10-20',
 '2011-11-07',
 '2013-04-24',
 '2014-03-18',
 '2015-01-15',
 '2015-11-16',
 '2016-06-24',
 '2016-11-09',
 '2018-05-08',
 '2018-10-29',
 '2019-03-15',
 '2019-05-24',
 '2020-01-23',
 '2020-02-21',
 '2020-06-30',
 '2020-11-09',
 '2021-02-01',
 '2021-03-23',
 '2021-04-15',
 '2022-02-24',
 '2022-03-17',
 '2022-09-19',
 '2022-11-30',
 '2023-02-06',
 '2023-04-17',
 '2023-10-09',
 '2023-11-20']



sectors = [
    "Industrials",
    "Healthcare",
    "Technology",
    "Financials",
    "Real Estate",
    "Consumer Cyclicals",
    "Basic Materials",
    "Consumer Non-Cyclicals",
    "Utilities",
    "Energy"
]






def call_function(INDEX,event_dates,constituent_data,fama_french,index_data,sectors):

    # Verschiedene Werte für die Parameter car_event1, car_event_lower und car_event_upper
    car_events = [
        {'car_event1': '(-3,3)', 'days_before': 104, 'days_after': 4},
        {'car_event1': '(-5,5)', 'days_before': 106, 'days_after': 6},
        {'car_event1': '(-2,5)', 'days_before': 103, 'days_after': 6},
        {'car_event1': '(-5,2)', 'days_before': 106, 'days_after': 3},
        {'car_event1': '(0,10)', 'days_before': 101, 'days_after': 11},
        {'car_event1': '(0,2)', 'days_before': 101, 'days_after': 3}
    ]
    
    
    # Initialisierung des DataFrames außerhalb der Schleifen
    
    result_df_TGRANK = pd.DataFrame(columns=['Event_Date','Event_Window','Sector','SU','Z','TGRANK'])
    result_df_SIGN_TEST = pd.DataFrame(columns=['Event_Date','Event_Window','Sector','Z_SIGN'])
    result_df_GENERALIZED_SIGN_TEST = pd.DataFrame(columns=['Event_Date','Event_Window','Sector','W','P_HAT','Z_GS_SIGN'])
    result_df_TEST_STATISTICS = pd.DataFrame(columns=['Event_Date','Event_Window','Sector','T_GRANK','Z_GSIGN','Z_SIGN'])
    
    
    # Schleife über die verschiedenen Event-Daten
    
    for sector in sectors:
        for event_date in event_dates:
            for car_event in car_events:
                #car_event_val = car_event['car_event1']
    
        
                # Ausführung der regression Funktion
                final_TGRANK,final_SIGN_TEST,final_GENERALIZED_SIGN_TEST,final_TEST_STATISTICS = regression_and_calc(master_file=master.copy(),
                                       event_date=event_date,
                                       index_ric=INDEX,
                                       constituent_data=constituent_data,
                                       index_data=index_data,
                                       fama_french=fama_french,
                                       sector = sector,
                                       **car_event)
                
                
                
                result_df_TGRANK = result_df_TGRANK.append(final_TGRANK, ignore_index=True)
                result_df_SIGN_TEST = result_df_SIGN_TEST.append(final_SIGN_TEST, ignore_index=True)
                result_df_GENERALIZED_SIGN_TEST = result_df_GENERALIZED_SIGN_TEST.append(final_GENERALIZED_SIGN_TEST, ignore_index=True)
                result_df_TEST_STATISTICS = result_df_TEST_STATISTICS.append(final_TEST_STATISTICS, ignore_index=True)
            
    
    return result_df_TGRANK,result_df_SIGN_TEST,result_df_GENERALIZED_SIGN_TEST,result_df_TEST_STATISTICS
    


TGRANK_SSHI_SECTORS,SIGN_TEST_SSHI_SECTORS,GENERALIZED_SIGN_TEST_SSHI_SECTORS,TEST_STATISTICS_SSHI_SECTORS = call_function(INDEX='SSHI',event_dates=event_dates_switzerland,constituent_data=SSHI,fama_french=fama_french_europe,index_data=SSHI_index,sectors=sectors)
TGRANK_SSHI_SECTORS.to_excel('TGRANK_SSHI_SECTORS.xlsx',index=False)
SIGN_TEST_SSHI_SECTORS.to_excel('SIGN_TEST_SSHI_SECTORS.xlsx',index=False)
GENERALIZED_SIGN_TEST_SSHI_SECTORS.to_excel('GENERALIZED_SIGN_TEST_SSHI_SECTORS.xlsx',index=False)
TEST_STATISTICS_SSHI_SECTORS.to_excel('TEST_STATISTICS_SSHI_SECTORS.xlsx',index=False)


TGRANK_STOXX_SECTORS,SIGN_TEST_STOXX_SECTORS,GENERALIZED_SIGN_TEST_STOXX_SECTORS,TEST_STATISTICS_STOXX_SECTORS = call_function(INDEX='STOXX',event_dates=event_dates_eu,constituent_data=STOXX,fama_french=fama_french_europe,index_data=STOXX_index,sectors=sectors)
TGRANK_STOXX_SECTORS.to_excel('TGRANK_STOXX_SECTORS.xlsx',index=False)
SIGN_TEST_STOXX_SECTORS.to_excel('SIGN_TEST_STOXX_SECTORS.xlsx',index=False)
GENERALIZED_SIGN_TEST_STOXX_SECTORS.to_excel('GENERALIZED_SIGN_TEST_STOXX_SECTORS.xlsx',index=False)
TEST_STATISTICS_STOXX_SECTORS.to_excel('TEST_STATISTICS_STOXX_SECTORS.xlsx',index=False)

TGRANK_SPX_SECTORS,SIGN_TEST_SPX_SECTORS,GENERALIZED_SIGN_TEST_SPX_SECTORS,TEST_STATISTICS_SPX_SECTORS = call_function(INDEX='SPX',event_dates=event_dates_us,constituent_data=SPX,fama_french=fama_french_us,index_data=SPX_index,sectors=sectors)
TGRANK_SPX_SECTORS.to_excel('TGRANK_SPX_SECTORS.xlsx',index=False)
SIGN_TEST_SPX_SECTORS.to_excel('SIGN_TEST_SPX_SECTORS.xlsx',index=False)
GENERALIZED_SIGN_TEST_SPX_SECTORS.to_excel('GENERALIZED_SIGN_TEST_SPX_SECTORS.xlsx',index=False)
TEST_STATISTICS_SPX_SECTORS.to_excel('TEST_STATISTICS_SPX_SECTORS.xlsx.xlsx',index=False)


master_test = master.copy()

master_new = master_test.reset_index(inplace=False)

sectors = master_new['TRBC Economic Sector Name'].unique()

unique_values_df = pd.DataFrame(columns=event_dates_switzerland)

for date in event_dates_switzerland:
    
    filtered_data = master_new[(master_new['Date'] == date) & (master_new['Index'] == 'SSHI')]
    
    unique_values = filtered_data['TRBC Economic Sector Name'].unique()
    
    for value in unique_values:
    
        unique_values_df[date] = pd.Series(unique_values)






