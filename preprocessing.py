#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 17:23:47 2024

@author: kevinfrank
"""

import pandas as pd
import refinitiv.data as rd
import time as time

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



indices = {
    "SPI SWISS PERFORMANCE INDEX": "SSHI",
    "S&P 500 INDEX": "SPX",
    "STOXX EUROPE 600 EUR PRICE INDEX": "STOXX"
}

def indexconst_and_data(ereignisse, indices):
    const_indices = pd.DataFrame(columns=['Date','RIC','GICS Sector Name','Index'])
    for name, index in indices.items():
        for event, date in ereignisse.items():
            const_indices_temp = rd.get_data(universe=f"0#.{index}({date})", fields=["TR.RIC","TR.GICSSector"])
            const_indices_temp['Date'] = date
            const_indices_temp['Index'] = index
            const_indices = const_indices.append(const_indices_temp[['Date','RIC','GICS Sector Name','Index']], ignore_index=True)
    return const_indices

instruments = indexconst_and_data(ereignisse,indices)

unique_instruments = instruments['RIC'].unique()

unique_instruments = pd.DataFrame({'RIC': unique_instruments})

ric_list = unique_instruments['RIC']

instruments.to_excel('instruments.xlsx', index=False)

ric_list.to_excel('instruments_unique.xlsx', index=False)

data_list = []
for index, ric in enumerate(ric_list):
    while True:
        try:
            rd.open_session()
            data = rd.get_history(universe=ric, fields='TR.CLOSEPRICE', interval="daily", start="01-04-2010", end="12-31-2023")
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







#data = rd.get_history(universe="ABBN.S", fields=['TR.CLOSEPRICE','TR.CompanyMarketCap','TR.Volume'], interval="daily", start="01-01-2010", end="12-31-2010")
#data = rd.get_data(universe="ABBN.S", fields='TR.GICSSector')
#data = rd.get_history(universe=['ABBN.S','ADXN.S'], fields=['TR.CLOSEPRICE','TR.CompanyMarketCap','TR.Volume'], interval="daily", start="01-04-2010", end="01-05-2010")
#dataframe = pd.DataFrame(columns=['Close Price','Company Market Cap','Volume'])











