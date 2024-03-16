#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 08:44:31 2023

@author: kevinfrank
"""

"Test"


# 0#.SSHI    --> SPI SWISS PERFORMANCE...
# 0#.SPX     --> S&P 500 INDEX
# 0#.STOXX   --> STOXX EUROPE 600 EUR PRICE...


import pandas as pd
import refinitiv.data as rd
import time

ereignisse = {
    "Flugverbot Eyjafjallajökull": "2010-04-15",
    "Arabischer Frühling": "2010-12-17",
    "Nuklearkatastrophe Fukushima": "2011-03-11",
    "Sturz von Muammar al-Gaddafi": "2011-10-20",
    "Tod von Steve Jobs": "2011-11-05",
    "Einbruch Textilfabrik Bangladesch": "2013-04-24",
    "Annexion der Krim durch Russland": "2014-03-18",
    "Aufhebung des CHF-Mindestkurs durch die SNB": "2015-01-15",
    "Schießereien und Explosionen in Paris": "2015-11-13",
    "Abstimmung zum Brexit-Referendum im Vereinigten Königreich": "2016-06-24",
    "Wahl von Donald Trump zum US-Präsidenten": "2016-11-09",
    "Ankündigung der Handelszölle gegen China durch US-Präsident Donald Trump": "2018-03-22",
    "Kündigung des Atom-Abkommen mit dem Iran durch die USA": "2018-05-08",
    "Ankündigung der Handelszölle gegen USA durch China": "2018-03-22",
    "Wahl von Jair Bolsonaro zum brasilianischen Präsidenten": "2018-10-28",
    "Terroranschlag auf zwei Moscheen in Neuseeland": "2019-03-15",
    "Ankündigung zum Rücktritt von Theresa May": "2019-05-24",
    "Ankündigung 1. Lockdown in China aufgrund der Covid-19-Pandemie": "2020-01-23",
    "Erste bestätigten Covid-19-Fälle in der europäischen Region der WHO": "2020-02-21",
    "Verabschiedung des chinesischen Sicherheitsgesetz für Hongkong": "2020-06-30",
    "Wahl von Joe Biden zum US-Präsidenten": "2020-11-07",
    "Entmachtung der Zivilregierung in Myanmar durch die Militärjunta": "2021-02-01",
    "Blockierung des Suez-Kanal durch das Containerschiff Ever Given": "2021-03-23",
    "Ankündigung zum Abzug der US-Truppen aus Afghanistan": "2021-04-14",
    "Russischer Einmarsch in Ukraine": "2022-02-24",
    "1. Leitzinserhöhung der FED im 2022": "2022-03-17",
    "Start der Proteste gegen die Regierung im Iran": "2022-09-17",
    "Einführung von ChatGPT 3.5 durch OpenAI": "2022-11-30",
    "Erdbeben in der Türkei und Syrien": "2023-02-06",
    "Start des Krieges im Sudan": "2023-04-15",
    "Terrorangriff der Hamas auf Israel": "2023-10-07",
    "Wahl von Javier Milei zum argentinischen Präsidenten": "2023-11-19"
}


def indexconst_and_data(index,fields, start, end, interval):
    data_list = []
    rd.open_session()
    const_indices = rd.get_data(universe=index, fields=fields)
    
    ric_list = const_indices['Instrument']
   
    data_list = []
    volume_data_list = []
    
    start_time = time.time()
    
    for index, ric in enumerate(ric_list):
        while True:
            try:
                data = rd.get_history(universe=ric, fields=['TR.TotalReturn'], interval=interval, start=start, end=end)
                data_list.append(data)
                
                volume_data = rd.get_history(universe=ric, fields=['TR.Volume'], interval=interval, start=start, end=end)
                volume_data_list.append(volume_data)
                
                
                data.columns = [ric]
                print(ric)
                print(len(ric_list) - index)
                break
            except Exception as e:
                print(f"Fehler beim Abrufen von {ric}: {str(e)}")
                time.sleep(1)
 
    
    historical_data_df = pd.concat(data_list, axis=1)
    volume_data_df = pd.concat(volume_data_list, axis=1)
    
    end_time = time.time()
    duration = end_time - start_time
    
    rd.close_session()
    
    
    return const_indices,historical_data_df,volume_data_df,duration


flugverbot_const_SSHI_2010, flugverbot_return_SSHI_2010,flugverbot_volume_SSHI_2010,flugverbot_duration_SSHI_2010 = indexconst_and_data(index=['0#.SSHI(2010-03-18)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry', "TR.TRBCEconomicSector"],start='2010-03-18',end='2010-05-13',interval='daily')
flugverbot_const_SPX_2010, flugverbot_return_SPX_2010,flugverbot_volume_SPX_2010 ,flugverbot_duration_SPX_2010  = indexconst_and_data(index=['0#.SPX(2010-03-18)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry', "TR.TRBCEconomicSector"],start='2010-03-18',end='2010-05-13',interval='daily')
flugverbot_const_STOXX_2010, flugverbot_return_STOXX_2010,flugverbot_volume_STOXX_2010 ,flugverbot_duration_STOXX_2010  = indexconst_and_data(index=['0#.STOXX(2010-03-18)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry', "TR.TRBCEconomicSector"],start='2010-03-18',end='2010-05-13',interval='daily')

hamas_const_SSHI_2023, hamas_return_SSHI_2023,hamas_volume_STOXX_2023,hamas_duration_SSHI_2023 = indexconst_and_data(index=['0#.SSHI(2023-09-11)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry', "TR.TRBCEconomicSector"],start='2023-09-11',end='2023-11-03',interval='daily')
hamas_const_SPX_2023, hamas_return_SPX_2023,hamas_volume_SPX_2023,hamas_duration_SPX_2023  = indexconst_and_data(index=['0#.SPX(2023-09-11)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry', "TR.TRBCEconomicSector"],start='2023-09-11',end='2023-11-03',interval='daily')
hamas_const_STOXX_2023, hamas_return_STOXX_2023,hamas_volume_STOXX_2023 ,hamas_duration_STOXX_2023  = indexconst_and_data(index=['0#.STOXX(2023-09-11)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry', "TR.TRBCEconomicSector"],start='2023-09-11',end='2023-11-03',interval='daily')

rd.open_session()
description = rd.get_data('ABBN.S',['TR.TotalReturn.description','TR.Volume.description',"TR.CompanyName.description", "TR.GICSSector.description", 'TR.GICSIndustry.description', "TR.TRBCEconomicSector.description"])


def mising_values(data):
    
    values = data.shape[0]*data.shape[1]
    missing_values = data.isna().sum().sum()
    calc = round(100-(values - missing_values)/values*100,2)
    
    print(calc)



mising_values(flugverbot_return_SSHI_2010)
mising_values(flugverbot_return_SPX_2010)
mising_values(flugverbot_return_STOXX_2010)

mising_values(hamas_return_SSHI_2023)
mising_values(hamas_return_SPX_2023)
mising_values(hamas_return_STOXX_2023)


excel_file = 'flugverbot_const_SSHI_2010.xlsx'
flugverbot_const_SSHI_2010.to_excel(excel_file, index=False)

excel_file = 'flugverbot_return_SSHI_2010.xlsx'
flugverbot_return_SSHI_2010.to_excel(excel_file, index=True)

excel_file = 'flugverbot_volume_SSHI_2010.xlsx'
flugverbot_volume_SSHI_2010.to_excel(excel_file, index=True)


excel_file = 'hamas_const_SSHI_2023.xlsx'
hamas_const_SSHI_2023.to_excel(excel_file, index=False)

excel_file = 'hamas_return_SSHI_2023.xlsx'
hamas_return_SSHI_2023.to_excel(excel_file, index=True)

excel_file = 'hamas_volume_STOXX_2023.xlsx'
hamas_volume_STOXX_2023.to_excel(excel_file, index=True)






