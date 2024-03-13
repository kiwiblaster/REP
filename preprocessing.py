#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 08:44:31 2023

@author: kevinfrank
"""


import pandas as pd
import refinitiv.data as rd
import time
from fredapi import Fred
import os as os


def indexconst_and_data(index,fields, start, end, interval):
    data_list = []
    rd.open_session()
    const_indices = rd.get_data(universe=index, fields=fields)
    
    
    const_indices = const_indices.drop(const_indices[const_indices['Instrument'] == 'ACK.N'].index)
    const_indices = const_indices.drop(const_indices[const_indices['Instrument'] == 'PZS.N^L99'].index)
    const_indices = const_indices.drop(const_indices[const_indices['Instrument'] == 'RTN.N^L97'].index)
    const_indices = const_indices.drop(const_indices[const_indices['Instrument'] == 'ALT.N^K99'].index)
    const_indices = const_indices.drop(const_indices[const_indices['Instrument'] == 'FSR.N^B01'].index)
    const_indices = const_indices.drop(const_indices[const_indices['Instrument'] == 'FLR_W.N^L00'].index)
    const_indices = const_indices.drop(const_indices[const_indices['Instrument'] == 'FLR_w.N^L00'].index)
    const_indices = const_indices.drop(const_indices[const_indices['Instrument'] == 'RATL.OQ'].index)
    
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
const_DJA_1996, data_DJA_1996  = indexconst_and_data(index=['0#.DJA(1996-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='1996-1-1',end='1996-12-31',interval='daily')
const_DJA_1997, data_DJA_1997  = indexconst_and_data(index=['0#.DJA(1997-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='1997-1-1',end='1997-12-31',interval='daily')
const_DJA_1998, data_DJA_1998  = indexconst_and_data(index=['0#.DJA(1998-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='1998-1-1',end='1998-12-31',interval='daily')
const_DJA_1999, data_DJA_1999  = indexconst_and_data(index=['0#.DJA(1999-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='1999-1-1',end='1999-12-31',interval='daily')
const_DJA_2000, data_DJA_2000  = indexconst_and_data(index=['0#.DJA(2000-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2000-1-1',end='2000-12-31',interval='daily')
const_DJA_2001, data_DJA_2001  = indexconst_and_data(index=['0#.DJA(2001-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2001-1-1',end='2001-12-31',interval='daily')
const_DJA_2002, data_DJA_2002  = indexconst_and_data(index=['0#.DJA(2002-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2002-1-1',end='2002-12-31',interval='daily')
const_DJA_2003, data_DJA_2003  = indexconst_and_data(index=['0#.DJA(2003-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2003-1-1',end='2003-12-31',interval='daily')
const_DJA_2004, data_DJA_2004  = indexconst_and_data(index=['0#.DJA(2004-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2004-1-1',end='2004-12-31',interval='daily')
const_DJA_2005, data_DJA_2005  = indexconst_and_data(index=['0#.DJA(2005-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2005-1-1',end='2005-12-31',interval='daily')
const_DJA_2006, data_DJA_2006  = indexconst_and_data(index=['0#.DJA(2006-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2006-1-1',end='2006-12-31',interval='daily')
const_DJA_2007, data_DJA_2007  = indexconst_and_data(index=['0#.DJA(2007-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2007-1-1',end='2007-12-31',interval='daily')
const_DJA_2008, data_DJA_2008  = indexconst_and_data(index=['0#.DJA(2008-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2008-1-1',end='2008-12-31',interval='daily')
const_DJA_2009, data_DJA_2009  = indexconst_and_data(index=['0#.DJA(2009-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2009-1-1',end='2009-12-31',interval='daily')
const_DJA_2010, data_DJA_2010  = indexconst_and_data(index=['0#.DJA(2010-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2010-1-1',end='2010-12-31',interval='daily')
const_DJA_2011, data_DJA_2011  = indexconst_and_data(index=['0#.DJA(2011-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2011-1-1',end='2011-12-31',interval='daily')
const_DJA_2012, data_DJA_2012  = indexconst_and_data(index=['0#.DJA(2012-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2012-1-1',end='2012-12-31',interval='daily')
const_DJA_2013, data_DJA_2013  = indexconst_and_data(index=['0#.DJA(2013-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2013-1-1',end='2013-12-31',interval='daily')
const_DJA_2014, data_DJA_2014  = indexconst_and_data(index=['0#.DJA(2014-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2014-1-1',end='2014-12-31',interval='daily')
const_DJA_2015, data_DJA_2015  = indexconst_and_data(index=['0#.DJA(2015-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2015-1-1',end='2015-12-31',interval='daily')
const_DJA_2016, data_DJA_2016  = indexconst_and_data(index=['0#.DJA(2016-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2016-1-1',end='2016-12-31',interval='daily')
const_DJA_2017, data_DJA_2017  = indexconst_and_data(index=['0#.DJA(2017-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2017-1-1',end='2017-12-31',interval='daily')
const_DJA_2018, data_DJA_2018  = indexconst_and_data(index=['0#.DJA(2018-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2018-1-1',end='2018-12-31',interval='daily')
const_DJA_2019, data_DJA_2019  = indexconst_and_data(index=['0#.DJA(2019-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2019-1-1',end='2019-12-31',interval='daily')
const_DJA_2020, data_DJA_2020  = indexconst_and_data(index=['0#.DJA(2020-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2020-1-1',end='2020-12-31',interval='daily')
const_DJA_2021, data_DJA_2021  = indexconst_and_data(index=['0#.DJA(2021-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2021-1-1',end='2021-12-31',interval='daily')
const_DJA_2022, data_DJA_2022  = indexconst_and_data(index=['0#.DJA(2022-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2022-1-1',end='2022-12-31',interval='daily')
const_DJA_2023, data_DJA_2023  = indexconst_and_data(index=['0#.DJA(2023-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2023-1-1',end='2023-12-31',interval='daily')



const_SPX_1995, data_SPX_1995  = indexconst_and_data(index=['0#.SPX(1995-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='1995-1-1',end='1995-12-31',interval='daily')
const_SPX_1996, data_SPX_1996  = indexconst_and_data(index=['0#.SPX(1996-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='1996-1-1',end='1996-12-31',interval='daily')
const_SPX_1997, data_SPX_1997  = indexconst_and_data(index=['0#.SPX(1997-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='1997-1-1',end='1997-12-31',interval='daily')
const_SPX_1998, data_SPX_1998  = indexconst_and_data(index=['0#.SPX(1998-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='1998-1-1',end='1998-12-31',interval='daily')
const_SPX_1999, data_SPX_1999  = indexconst_and_data(index=['0#.SPX(1999-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='1999-1-1',end='1999-12-31',interval='daily')
const_SPX_2000, data_SPX_2000  = indexconst_and_data(index=['0#.SPX(2000-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2000-1-1',end='2000-12-31',interval='daily')
const_SPX_2001, data_SPX_2001  = indexconst_and_data(index=['0#.SPX(2001-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2001-1-1',end='2001-12-31',interval='daily')
const_SPX_2002, data_SPX_2002  = indexconst_and_data(index=['0#.SPX(2002-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2002-1-1',end='2002-12-31',interval='daily')
const_SPX_2003, data_SPX_2003  = indexconst_and_data(index=['0#.SPX(2003-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2003-1-1',end='2003-12-31',interval='daily')
const_SPX_2004, data_SPX_2004  = indexconst_and_data(index=['0#.SPX(2004-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2004-1-1',end='2004-12-31',interval='daily')
const_SPX_2005, data_SPX_2005  = indexconst_and_data(index=['0#.SPX(2005-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2005-1-1',end='2005-12-31',interval='daily')
const_SPX_2006, data_SPX_2006  = indexconst_and_data(index=['0#.SPX(2006-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2006-1-1',end='2006-12-31',interval='daily')
const_SPX_2007, data_SPX_2007  = indexconst_and_data(index=['0#.SPX(2007-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2007-1-1',end='2007-12-31',interval='daily')
const_SPX_2008, data_SPX_2008  = indexconst_and_data(index=['0#.SPX(2008-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2008-1-1',end='2008-12-31',interval='daily')
const_SPX_2009, data_SPX_2009  = indexconst_and_data(index=['0#.SPX(2009-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2009-1-1',end='2009-12-31',interval='daily')
const_SPX_2010, data_SPX_2010  = indexconst_and_data(index=['0#.SPX(2010-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2010-1-1',end='2010-12-31',interval='daily')
const_SPX_2011, data_SPX_2011  = indexconst_and_data(index=['0#.SPX(2011-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2011-1-1',end='2011-12-31',interval='daily')
const_SPX_2012, data_SPX_2012  = indexconst_and_data(index=['0#.SPX(2012-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2012-1-1',end='2012-12-31',interval='daily')
const_SPX_2013, data_SPX_2013  = indexconst_and_data(index=['0#.SPX(2013-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2013-1-1',end='2013-12-31',interval='daily')
const_SPX_2014, data_SPX_2014  = indexconst_and_data(index=['0#.SPX(2014-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2014-1-1',end='2014-12-31',interval='daily')
const_SPX_2015, data_SPX_2015  = indexconst_and_data(index=['0#.SPX(2015-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2015-1-1',end='2015-12-31',interval='daily')
const_SPX_2016, data_SPX_2016  = indexconst_and_data(index=['0#.SPX(2016-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2016-1-1',end='2016-12-31',interval='daily')
const_SPX_2017, data_SPX_2017  = indexconst_and_data(index=['0#.SPX(2017-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2017-1-1',end='2017-12-31',interval='daily')
const_SPX_2018, data_SPX_2018  = indexconst_and_data(index=['0#.SPX(2018-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2018-1-1',end='2018-12-31',interval='daily')
const_SPX_2019, data_SPX_2019  = indexconst_and_data(index=['0#.SPX(2019-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2019-1-1',end='2019-12-31',interval='daily')
const_SPX_2020, data_SPX_2020  = indexconst_and_data(index=['0#.SPX(2020-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2020-1-1',end='2020-12-31',interval='daily')
const_SPX_2021, data_SPX_2021  = indexconst_and_data(index=['0#.SPX(2021-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2021-1-1',end='2021-12-31',interval='daily')
const_SPX_2022, data_SPX_2022  = indexconst_and_data(index=['0#.SPX(2022-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2022-1-1',end='2022-12-31',interval='daily')
const_SPX_2023, data_SPX_2023  = indexconst_and_data(index=['0#.SPX(2023-01-01)'],fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'],start='2023-1-1',end='2023-12-31',interval='daily')


rd.open_session()
SPX_daily = rd.get_history(universe=".SPX", fields='TR.PriceCLose', interval='daily', start='1998-1-31', end='2023-08-31')
SPX_monthly = rd.get_history(universe=".SPX", fields='TR.PriceCLose', interval='monthly', start='1978-2-28', end='2023-09-30')

DJA_daily = rd.get_history(universe=".DJA", fields='TR.PriceCLose', interval='daily', start='1998-1-31', end='2023-08-31')
DJA_monthly = rd.get_history(universe=".DJA", fields='TR.PriceCLose', interval='monthly', start='1998-1-31', end='2023-08-31')

rd.open_session()
const_SPX_2007 = rd.get_data(universe='0#.SPX(2007-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2008 = rd.get_data(universe='0#.SPX(2008-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2008 = rd.get_data(universe='0#.SPX(2008-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2009 = rd.get_data(universe='0#.SPX(2009-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2010 = rd.get_data(universe='0#.SPX(2010-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2011 = rd.get_data(universe='0#.SPX(2011-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2012 = rd.get_data(universe='0#.SPX(2012-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2013 = rd.get_data(universe='0#.SPX(2013-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2014 = rd.get_data(universe='0#.SPX(2014-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2015 = rd.get_data(universe='0#.SPX(2015-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2016 = rd.get_data(universe='0#.SPX(2016-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2017 = rd.get_data(universe='0#.SPX(2017-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2018 = rd.get_data(universe='0#.SPX(2018-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2019 = rd.get_data(universe='0#.SPX(2019-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2020 = rd.get_data(universe='0#.SPX(2020-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2021 = rd.get_data(universe='0#.SPX(2021-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2022 = rd.get_data(universe='0#.SPX(2022-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
const_SPX_2023 = rd.get_data(universe='0#.SPX(2023-01-01)',fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])



const_SPX_2007 = const_SPX_2007[const_SPX_2007['GICS Sector Name'] != '']
const_SPX_2008 = const_SPX_2008[const_SPX_2008['GICS Sector Name'] != '']
const_SPX_2009 = const_SPX_2009[const_SPX_2009['GICS Sector Name'] != '']
const_SPX_2010 = const_SPX_2010[const_SPX_2010['GICS Sector Name'] != '']
const_SPX_2011 = const_SPX_2011[const_SPX_2011['GICS Sector Name'] != '']
const_SPX_2012 = const_SPX_2012[const_SPX_2012['GICS Sector Name'] != '']
const_SPX_2013 = const_SPX_2013[const_SPX_2013['GICS Sector Name'] != '']
const_SPX_2014 = const_SPX_2014[const_SPX_2014['GICS Sector Name'] != '']
const_SPX_2015 = const_SPX_2015[const_SPX_2015['GICS Sector Name'] != '']
const_SPX_2016 = const_SPX_2016[const_SPX_2016['GICS Sector Name'] != '']
const_SPX_2017 = const_SPX_2017[const_SPX_2017['GICS Sector Name'] != '']
const_SPX_2018 = const_SPX_2018[const_SPX_2018['GICS Sector Name'] != '']
const_SPX_2019 = const_SPX_2019[const_SPX_2019['GICS Sector Name'] != '']
const_SPX_2020 = const_SPX_2020[const_SPX_2020['GICS Sector Name'] != '']
const_SPX_2021 = const_SPX_2021[const_SPX_2021['GICS Sector Name'] != '']
const_SPX_2022 = const_SPX_2022[const_SPX_2022['GICS Sector Name'] != '']
const_SPX_2023 = const_SPX_2023[const_SPX_2023['GICS Sector Name'] != '']


def create_sector_equities_df(df):
   
    dummy_df = pd.get_dummies(df['GICS Sector Name'])
    dummy_df.insert(0, 'Instrument', df['Instrument'])
    result_df = dummy_df.groupby('Instrument').sum()
    result_df[result_df > 1] = 1

    return result_df

sector_equities_2007 = create_sector_equities_df(const_SPX_2007)
sector_equities_2008 = create_sector_equities_df(const_SPX_2008)
sector_equities_2009 = create_sector_equities_df(const_SPX_2009)
sector_equities_2010 = create_sector_equities_df(const_SPX_2010)
sector_equities_2011 = create_sector_equities_df(const_SPX_2011)
sector_equities_2012 = create_sector_equities_df(const_SPX_2012)
sector_equities_2013 = create_sector_equities_df(const_SPX_2013)
sector_equities_2014 = create_sector_equities_df(const_SPX_2014)
sector_equities_2015 = create_sector_equities_df(const_SPX_2015)
sector_equities_2016 = create_sector_equities_df(const_SPX_2016)
sector_equities_2017 = create_sector_equities_df(const_SPX_2017)
sector_equities_2018 = create_sector_equities_df(const_SPX_2018)
sector_equities_2019 = create_sector_equities_df(const_SPX_2019)
sector_equities_2020 = create_sector_equities_df(const_SPX_2020)
sector_equities_2021 = create_sector_equities_df(const_SPX_2021)
sector_equities_2022 = create_sector_equities_df(const_SPX_2022)
sector_equities_2023 = create_sector_equities_df(const_SPX_2023)

data_SPX_2007 = pd.read_excel('data_SPX_2007.xlsx', index_col='Date')
data_SPX_2008 = pd.read_excel('data_SPX_2008.xlsx', index_col='Date')
data_SPX_2009 = pd.read_excel('data_SPX_2009.xlsx', index_col='Date')
data_SPX_2010 = pd.read_excel('data_SPX_2010.xlsx', index_col='Date')
data_SPX_2011 = pd.read_excel('data_SPX_2011.xlsx', index_col='Date')
data_SPX_2012 = pd.read_excel('data_SPX_2012.xlsx', index_col='Date')
data_SPX_2013 = pd.read_excel('data_SPX_2013.xlsx', index_col='Date')
data_SPX_2014 = pd.read_excel('data_SPX_2014.xlsx', index_col='Date')
data_SPX_2015 = pd.read_excel('data_SPX_2015.xlsx', index_col='Date')
data_SPX_2016 = pd.read_excel('data_SPX_2016.xlsx', index_col='Date')
data_SPX_2017 = pd.read_excel('data_SPX_2017.xlsx', index_col='Date')
data_SPX_2018 = pd.read_excel('data_SPX_2018.xlsx', index_col='Date')
data_SPX_2019 = pd.read_excel('data_SPX_2019.xlsx', index_col='Date')
data_SPX_2020 = pd.read_excel('data_SPX_2020.xlsx', index_col='Date')
data_SPX_2021 = pd.read_excel('data_SPX_2021.xlsx', index_col='Date')
data_SPX_2022 = pd.read_excel('data_SPX_2022.xlsx', index_col='Date')
data_SPX_2023 = pd.read_excel('data_SPX_2023.xlsx', index_col='Date')

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


data_SPX_2007, sector_2007 = final_datasets(data_SPX_2007,const_SPX_2007,sector_equities_2007,2007)
data_SPX_2008, sector_2008 = final_datasets(data_SPX_2008,const_SPX_2008,sector_equities_2008,2008)
data_SPX_2009, sector_2009 = final_datasets(data_SPX_2009,const_SPX_2009,sector_equities_2009,2009)
data_SPX_2010, sector_2010 = final_datasets(data_SPX_2010,const_SPX_2010,sector_equities_2010,2010)
data_SPX_2011, sector_2011 = final_datasets(data_SPX_2011,const_SPX_2011,sector_equities_2011,2011)
data_SPX_2012, sector_2012 = final_datasets(data_SPX_2012,const_SPX_2012,sector_equities_2012,2012)
data_SPX_2013, sector_2013 = final_datasets(data_SPX_2013,const_SPX_2013,sector_equities_2013,2013)
data_SPX_2014, sector_2014 = final_datasets(data_SPX_2014,const_SPX_2014,sector_equities_2014,2014)
data_SPX_2015, sector_2015 = final_datasets(data_SPX_2015,const_SPX_2015,sector_equities_2015,2015)
data_SPX_2016, sector_2016 = final_datasets(data_SPX_2016,const_SPX_2016,sector_equities_2016,2016)
data_SPX_2017, sector_2017 = final_datasets(data_SPX_2017,const_SPX_2017,sector_equities_2017,2017)
data_SPX_2018, sector_2018 = final_datasets(data_SPX_2018,const_SPX_2018,sector_equities_2018,2018)
data_SPX_2019, sector_2019 = final_datasets(data_SPX_2019,const_SPX_2019,sector_equities_2019,2019)
data_SPX_2020, sector_2020 = final_datasets(data_SPX_2020,const_SPX_2020,sector_equities_2020,2020)
data_SPX_2021, sector_2021 = final_datasets(data_SPX_2021,const_SPX_2021,sector_equities_2021,2021)
data_SPX_2022, sector_2022 = final_datasets(data_SPX_2022,const_SPX_2022,sector_equities_2022,2022)
data_SPX_2023, sector_2023 = final_datasets(data_SPX_2023,const_SPX_2023,sector_equities_2023,2023)


def fred(API, Id, start, freq):
    f = Fred(api_key = API)
    df = f.get_series(Id, observation_start=start, frequency = freq,aggregation_method = "eop")
    return df

recession = fred('09706c7d09017ef4e8d14e20f081ceff','USREC','1970-1-31','m')



# Change the working directory to the folder containing the Excel files
# os.chdir(os.path.expanduser('~') + '\\OneDrive\\Desktop\\School\\ZHAW\\3_Semester\\ML\\Project\\Eco_Indicators\\Monthly')

# Get the current working directory
current_directory = os.getcwd()

# Define the path where you want to save the extracted columns as Excel files
# output_path = os.path.expanduser('~') + '\\OneDrive\\Desktop\\School\\ZHAW\\3_Semester\\ML\\Project\\Eco_Indicators\\Extracted_Columns_Excel'
output_path = os.getcwd()


# Create the output directory if it doesn't exist
os.makedirs(output_path, exist_ok=True)

# Initialize variables to keep track of the latest starting date
latest_starting_date = None
latest_starting_date_filename = None

# Loop through each file in the directory
for filename in os.listdir():
    if filename.endswith('.xlsx'):
        file_path = os.path.join(current_directory, filename)

        # Read the 'US' sheet from the Excel file
        us_data = pd.read_excel(file_path, sheet_name='US', header=None, skiprows=1)

        # Extract columns A and B
        result_df = us_data.iloc[:, :2]

        # Rename columns based on the filename (excluding extension)
        result_df.columns = ['Date', filename[:-5]]

        # Convert the dates in the 'Date' column to the desired format
        result_df['Date'] = pd.to_datetime(result_df['Date']).dt.strftime("%d.%m.%Y")

        # Save the extracted columns as a new Excel file in the output path
        output_filename = f"{filename[:-5]}_extracted_columns.xlsx"
        output_file_path = os.path.join(output_path, output_filename)

        # Save the DataFrame to the new Excel file
        result_df.to_excel(output_file_path, index=False)



# Change the working directory to the folder containing the Excel files
# os.chdir(os.path.expanduser('~') + '\\OneDrive\\Desktop\\School\\ZHAW\\3_Semester\\ML\\Project\\Eco_Indicators\\Extracted_Columns_Excel')

# Get the current working directory
# current_directory = os.getcwd()

# Define the path where you want to save the modified Excel files
modified_output_path = os.getcwd()

# Create the output directory if it doesn't exist
os.makedirs(modified_output_path, exist_ok=True)

# Initialize variables to keep track of the latest starting date
latest_starting_date = None
latest_starting_date_filename = None
# 
# Loop through each file in the directory to find the latest starting date across all files
for filename in os.listdir():
    if filename.endswith('_extracted_columns.xlsx'):
        file_path = os.path.join(current_directory, filename)

        # Read the extracted columns Excel file
        extracted_data = pd.read_excel(file_path)

        # Extract day, month, and year components
        extracted_data[['Day', 'Month', 'Year']] = extracted_data['Date'].str.split('.', expand=True)

        # Create a new 'Date' column
        extracted_data['Date'] = pd.to_datetime(extracted_data[['Year', 'Month', 'Day']], errors='coerce')

        # Filter rows with valid dates
        filtered_data = extracted_data.dropna(subset=['Date'])

        # Find the latest starting date across all files
        file_min_date = filtered_data['Date'].min()
        if latest_starting_date is None or file_min_date > latest_starting_date:
            latest_starting_date = file_min_date

# Print the latest starting date found across all files
print(f"The latest starting date across all files is: {latest_starting_date}")

# Loop through each file again for modification
for filename in os.listdir():
    if filename.endswith('_extracted_columns.xlsx'):
        file_path = os.path.join(current_directory, filename)

        # Read the extracted columns Excel file
        extracted_data = pd.read_excel(file_path)

        # Extract day, month, and year components
        extracted_data[['Day', 'Month', 'Year']] = extracted_data['Date'].str.split('.', expand=True)

        # Create a new 'Date' column
        extracted_data['Date'] = pd.to_datetime(extracted_data[['Year', 'Month', 'Day']], errors='coerce')

        # Filter rows with valid dates
        filtered_data = extracted_data.dropna(subset=['Date'])

        # Filter rows with dates later than or equal to the latest starting date
        filtered_data = filtered_data[filtered_data['Date'] >= latest_starting_date]

        # Save the modified DataFrame to the new Excel file in the modified output path
        output_filename = f"{filename[:-20]}_modified_columns.xlsx"
        output_file_path = os.path.join(modified_output_path, output_filename)
        filtered_data.to_excel(output_file_path, index=False)

# Change the working directory to the folder containing the modified Excel files
os.chdir(os.getcwd())

# Get the current working directory
current_directory = os.getcwd()

# Loop through each file in the directory
for filename in os.listdir():
    if filename.endswith('_modified_columns.xlsx'):
        file_path = os.path.join(current_directory, filename)

        # Read the modified columns Excel file
        modified_data = pd.read_excel(file_path)

        # Convert 'Date' column to datetime
        modified_data['Date'] = pd.to_datetime(modified_data['Date'], errors='coerce')

        # Overwrite the 'Date' column with a European format
        modified_data['Date'] = modified_data['Date'].dt.strftime("%d.%m.%Y")

        # Delete the 'Day', 'Month', and 'Year' columns
        modified_data.drop(['Day', 'Month', 'Year'], axis=1, inplace=True)

        # Save the modified DataFrame back to the Excel file
        modified_data.to_excel(file_path, index=False)


# Change the working directory to the folder containing the modified Excel files
os.chdir(os.getcwd())

# Get the current working directory
current_directory = os.getcwd()

# Initialize an empty DataFrame to store concatenated data
concatenated_data = pd.DataFrame()

# Loop through each file in the directory
for filename in os.listdir():
    if filename.endswith('_modified_columns.xlsx'):
        file_path = os.path.join(current_directory, filename)

        # Read the modified columns Excel file
        modified_data = pd.read_excel(file_path)

        # Extract the data column (column B)
        modified_data = modified_data.rename(columns={modified_data.columns[1]: filename[:-20]})

        # Merge the data into the master DataFrame based on the index
        if concatenated_data.empty:
            concatenated_data = modified_data
        else:
            concatenated_data = pd.merge(concatenated_data, modified_data[modified_data.columns[1]], left_index=True, right_index=True, how='outer')

# Save the concatenated DataFrame to a new Excel file
concatenated_output_path = os.path.expanduser(os.getcwd())
os.makedirs(concatenated_output_path, exist_ok=True)
concatenated_output_file_path = os.path.join(concatenated_output_path, 'concatenated_data.xlsx')
concatenated_data.to_excel(concatenated_output_file_path, index=False)

concatenated_data.isna().sum()

concatenated_data = concatenated_data.dropna(axis=0)

indicators = concatenated_data

indicators.set_index('Date', inplace=True)

indicators.index = pd.to_datetime(indicators.index)

recession = pd.DataFrame(recession)

recession = recession.rename(columns={0: 'Signal'})

data = indicators.join(recession)

data['NextMonthSignal'] = data['Signal'].shift(-1)

columns = data.columns.tolist()

columns_new = list(reversed(columns))

# DataFrame mit umgekehrter Reihenfolge der Spalten erstellen
df_final = data[columns_new]


df_final.to_excel("data.xlsx", index=True)


def export_dataframes_to_excel(string):
    global_vars = globals()

    
    dataframes = {var_name: var_value for var_name, var_value in global_vars.items() if isinstance(var_value, pd.DataFrame) and var_name.startswith(string)}

    
    for df_name, df in dataframes.items():
        file_name = f"{df_name}.xlsx"
        df.to_excel(file_name, index=True)
        print(f'DataFrame "{df_name}" wurde in {file_name} exportiert.')


export_dataframes_to_excel("data")




from functools import reduce

df_list = [data_SPX_2007, 
data_SPX_2008, 
data_SPX_2009, 
data_SPX_2010, 
data_SPX_2011, 
data_SPX_2012, 
data_SPX_2013, 
data_SPX_2014, 
data_SPX_2015, 
data_SPX_2016, 
data_SPX_2017, 
data_SPX_2018, 
data_SPX_2019, 
data_SPX_2020, 
data_SPX_2021, 
data_SPX_2022, 
data_SPX_2023,]

# Funktion, um die Spaltennamen aus einem DataFrame zu extrahieren
def get_column_names(df):
    return set(df.columns)

# Extrahieren der Spaltennamen aus allen DataFrames
column_names_sets = [get_column_names(df) for df in df_list]

# Finden der gemeinsamen Spaltennamen
common_columns = list(reduce(lambda a, b: a.intersection(b), column_names_sets))
len(common_columns)


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

const_SPX = indexconst_and_data(['GE.N', 'HBAN.OQ', 'SYY.N', 'LUMN.N', 'INTC.OQ', 'PG.N', 'CINF.OQ', 'ETN.N', 'MTB.N', 'PAYX.OQ', 'ELV.N', 'ZION.OQ', 'HON.OQ', 'BBY.N', 'TPR.N', 'KEY.N', 'LMT.N', 'AMP.N', 'MMC.N', 'MAS.N', 'D.N', 'SCHW.N', 'BSX.N', 'STT.N', 'GILD.OQ', 'TXT.N', 'L.N', 'CMI.N', 'FCX.N', 'DHR.N', 'BAX.N', 'AIG.N', 'TAP.N', 'CAT.N', 'TFC.N', 'LIN.OQ', 'TRV.N', 'NOC.N', 'SEE.N', 'ALL.N', 'AFL.N', 'BKR.OQ', 'SRE.N', 'PFE.N', 'CMCSA.OQ', 'SO.N', 'ECL.N', 'HUM.N', 'AVY.N', 'STZ.N', 'PNC.N', 'TMO.N', 'WFC.N', 'HAL.N', 'HD.N', 'IFF.N', 'NTAP.OQ', 'ED.N', 'TSN.N', 'MSFT.OQ', 'CMA.N', 'RTX.N', 'MMM.N', 'DIS.N', 'FDX.N', 'ZBH.N', 'KMB.N', 'WY.N', 'XEL.OQ', 'ADI.OQ', 'PCAR.OQ', 'JPM.N', 'MAR.OQ', 'GS.N', 'DUK.N', 'APD.N', 'VZ.N', 'CSCO.OQ', 'IP.N', 'UNP.N', 'AMZN.OQ', 'C.N', 'IPG.N', 'WHR.N', 'BDX.N', 'GWW.N', 'NVDA.OQ', 'COP.N', 'PNW.N', 'DRI.N', 'MCD.N', 'MCO.N', 'GIS.N', 'GEN.OQ', 'EXC.OQ', 'ADP.OQ', 'GPC.N', 'INTU.OQ', 'FE.N', 'HPQ.N', 'SPG.N', 'EA.OQ', 'BXP.N', 'RHI.N', 'DVN.N', 'AZO.N', 'NWL.OQ', 'UNH.N', 'USB.N', 'CNP.N', 'ITW.N', 'NUE.N', 'NEM.N', 'FI.N', 'NSC.N', 'SPGI.N', 'CAH.N', 'GLW.N', 'MSI.N', 'TGT.N', 'K.N', 'ABT.N', 'MO.N', 'CVX.N', 'F.N', 'EMN.N', 'FIS.N', 'MU.OQ', 'ROK.N', 'CSX.OQ', 'SNA.N', 'NI.N', 'KLAC.OQ', 'CI.N', 'LH.N', 'TJX.N', 'MRO.N', 'EOG.N', 'AMAT.OQ', 'COST.OQ', 'AEP.OQ', 'JNJ.N', 'QCOM.OQ', 'EL.N', 'LUV.N', 'WAT.N', 'PGR.N', 'FITB.OQ', 'BBWI.N', 'DHI.N', 'VNO.N', 'NKE.N', 'KIM.N', 'PPL.N', 'WBA.OQ', 'SBUX.OQ', 'PPG.N', 'HIG.N', 'HES.N', 'MRK.N', 'GD.N', 'MET.N', 'ETR.N', 'WM.N', 'ADBE.OQ', 'PEG.N', 'COF.N', 'GL.N', 'MKC.N', 'GOOGL.OQ', 'EQR.N', 'VFC.N', 'BMY.N', 'EMR.N', 'AON.N', 'KR.N', 'AES.N', 'YUM.N', 'A.N', 'CVS.N', 'DTE.N', 'HAS.OQ', 'PFG.OQ', 'VTRS.OQ', 'BALL.N', 'LNC.N', 'EIX.N', 'HWM.N', 'NTRS.OQ', 'VRSN.OQ', 'CAG.N', 'LEN.N', 'BK.N', 'CMS.N', 'SYK.N', 'VMC.N', 'AMGN.OQ', 'WMB.N', 'DOV.N', 'NEE.N', 'BA.N', 'LOW.N', 'T.N', 'XOM.N', 'PSA.N', 'VLO.N', 'EFX.N', 'APA.OQ', 'SWK.N', 'CTAS.OQ', 'AEE.N', 'CBRE.N', 'TROW.OQ', 'RVTY.N', 'MS.N', 'OMC.N', 'RF.N', 'KO.N', 'CME.OQ', 'MCK.N', 'IBM.N', 'JNPR.N', 'MDT.N', 'PEP.OQ', 'AXP.N', 'SLB.N', 'WMT.N', 'UPS.N', 'CL.N', 'SHW.N', 'TXN.OQ', 'CLX.N', 'HSY.N', 'EBAY.OQ', 'DGX.N', 'CPB.N', 'LLY.N', 'PHM.N', 'ADM.N', 'ADSK.OQ', 'CCL.N', 'PARA.OQ', 'BEN.N', 'ORCL.N', 'AAPL.OQ', 'OXY.N', 'PRU.N', 'DE.N', 'PH.N', 'BAC.N', 'COR.N', 'CTSH.OQ', 'BFb.N'], fields='TR.PriceCLose', start='2000-01-01', end='2023-08-31', interval='daily')
const_SPX.isna().sum().sum()
const_SPX_cleaned = const_SPX.dropna(how='all') # rows with all NA
const_SPX_cleaned = const_SPX_cleaned.dropna(axis=1) # columns with min. 1 NA
const_SPX_cleaned.isna().sum().sum() #0

rd.open_session()
const_indices = rd.get_data(['GE.N', 'HBAN.OQ', 'SYY.N', 'LUMN.N', 'INTC.OQ', 'PG.N', 'CINF.OQ', 'ETN.N', 'MTB.N', 'PAYX.OQ', 'ELV.N', 'ZION.OQ', 'HON.OQ', 'BBY.N', 'TPR.N', 'KEY.N', 'LMT.N', 'AMP.N', 'MMC.N', 'MAS.N', 'D.N', 'SCHW.N', 'BSX.N', 'STT.N', 'GILD.OQ', 'TXT.N', 'L.N', 'CMI.N', 'FCX.N', 'DHR.N', 'BAX.N', 'AIG.N', 'TAP.N', 'CAT.N', 'TFC.N', 'LIN.OQ', 'TRV.N', 'NOC.N', 'SEE.N', 'ALL.N', 'AFL.N', 'BKR.OQ', 'SRE.N', 'PFE.N', 'CMCSA.OQ', 'SO.N', 'ECL.N', 'HUM.N', 'AVY.N', 'STZ.N', 'PNC.N', 'TMO.N', 'WFC.N', 'HAL.N', 'HD.N', 'IFF.N', 'NTAP.OQ', 'ED.N', 'TSN.N', 'MSFT.OQ', 'CMA.N', 'RTX.N', 'MMM.N', 'DIS.N', 'FDX.N', 'ZBH.N', 'KMB.N', 'WY.N', 'XEL.OQ', 'ADI.OQ', 'PCAR.OQ', 'JPM.N', 'MAR.OQ', 'GS.N', 'DUK.N', 'APD.N', 'VZ.N', 'CSCO.OQ', 'IP.N', 'UNP.N', 'AMZN.OQ', 'C.N', 'IPG.N', 'WHR.N', 'BDX.N', 'GWW.N', 'NVDA.OQ', 'COP.N', 'PNW.N', 'DRI.N', 'MCD.N', 'MCO.N', 'GIS.N', 'GEN.OQ', 'EXC.OQ', 'ADP.OQ', 'GPC.N', 'INTU.OQ', 'FE.N', 'HPQ.N', 'SPG.N', 'EA.OQ', 'BXP.N', 'RHI.N', 'DVN.N', 'AZO.N', 'NWL.OQ', 'UNH.N', 'USB.N', 'CNP.N', 'ITW.N', 'NUE.N', 'NEM.N', 'FI.N', 'NSC.N', 'SPGI.N', 'CAH.N', 'GLW.N', 'MSI.N', 'TGT.N', 'K.N', 'ABT.N', 'MO.N', 'CVX.N', 'F.N', 'EMN.N', 'FIS.N', 'MU.OQ', 'ROK.N', 'CSX.OQ', 'SNA.N', 'NI.N', 'KLAC.OQ', 'CI.N', 'LH.N', 'TJX.N', 'MRO.N', 'EOG.N', 'AMAT.OQ', 'COST.OQ', 'AEP.OQ', 'JNJ.N', 'QCOM.OQ', 'EL.N', 'LUV.N', 'WAT.N', 'PGR.N', 'FITB.OQ', 'BBWI.N', 'DHI.N', 'VNO.N', 'NKE.N', 'KIM.N', 'PPL.N', 'WBA.OQ', 'SBUX.OQ', 'PPG.N', 'HIG.N', 'HES.N', 'MRK.N', 'GD.N', 'MET.N', 'ETR.N', 'WM.N', 'ADBE.OQ', 'PEG.N', 'COF.N', 'GL.N', 'MKC.N', 'GOOGL.OQ', 'EQR.N', 'VFC.N', 'BMY.N', 'EMR.N', 'AON.N', 'KR.N', 'AES.N', 'YUM.N', 'A.N', 'CVS.N', 'DTE.N', 'HAS.OQ', 'PFG.OQ', 'VTRS.OQ', 'BALL.N', 'LNC.N', 'EIX.N', 'HWM.N', 'NTRS.OQ', 'VRSN.OQ', 'CAG.N', 'LEN.N', 'BK.N', 'CMS.N', 'SYK.N', 'VMC.N', 'AMGN.OQ', 'WMB.N', 'DOV.N', 'NEE.N', 'BA.N', 'LOW.N', 'T.N', 'XOM.N', 'PSA.N', 'VLO.N', 'EFX.N', 'APA.OQ', 'SWK.N', 'CTAS.OQ', 'AEE.N', 'CBRE.N', 'TROW.OQ', 'RVTY.N', 'MS.N', 'OMC.N', 'RF.N', 'KO.N', 'CME.OQ', 'MCK.N', 'IBM.N', 'JNPR.N', 'MDT.N', 'PEP.OQ', 'AXP.N', 'SLB.N', 'WMT.N', 'UPS.N', 'CL.N', 'SHW.N', 'TXN.OQ', 'CLX.N', 'HSY.N', 'EBAY.OQ', 'DGX.N', 'CPB.N', 'LLY.N', 'PHM.N', 'ADM.N', 'ADSK.OQ', 'CCL.N', 'PARA.OQ', 'BEN.N', 'ORCL.N', 'AAPL.OQ', 'OXY.N', 'PRU.N', 'DE.N', 'PH.N', 'BAC.N', 'COR.N', 'CTSH.OQ', 'BFb.N'], fields=["TR.CompanyName", "TR.GICSSector", 'TR.GICSIndustry'])
sector_equities = create_sector_equities_df(const_indices)

const_SPX_all = const_SPX_cleaned

const_SPX_all = const_SPX_all.rename(columns={'BFb.N': 'BFB.N'})

def filter_dfs(df1, df2, common_instruments):
    # Filtern des ersten DataFrames für die gemeinsamen Spalten
    filtered_df1 = df1[common_instruments]

    # Filtern des zweiten DataFrames für die gemeinsamen Instrumente
    filtered_df2 = df2[df2['Instrument'].isin(common_instruments)]

    return filtered_df1, filtered_df2



# Gemeinsame Instrumente finden
common_instruments = set(const_SPX_all.columns).intersection(const_indices['Instrument'])

# Filtern der DataFrames
filtered_df1, filtered_df2 = filter_dfs(const_SPX_all, const_indices, common_instruments)

sector_equities_SPX = create_sector_equities_df(filtered_df2)

SPX_data_daily=filtered_df1

SPX_data_daily.to_excel('SPX_data_daily.xlsx', index=True)
sector_equities_SPX.to_excel('sector_equities_SPX.xlsx', index=True)                               
                                
counts = sector_equities_SPX.apply(lambda x: (x == 1).sum())

import pandas as pd
import matplotlib.pyplot as plt

# Erstelle einen Barplot
plt.figure(figsize=(12, 6))
counts.plot(kind='bar')
plt.title('Sector Distribution SPX')
plt.xlabel('Sector')
plt.ylabel('Frequency of Instruments')
plt.xticks()
plt.show()


rd.open_session()
ric = ['VOX','VCR','VDC','VDE','KCE','XLV','VIS','VGT','VAW','IYR','VPU',]
sector_etf = rd.get_history(universe=ric, fields='TR.CLOSEPRICE', interval='daily', start='2006-1-1', end='2023-08-31')

sector_etf.isna().sum().sum()

sector_etf = sector_etf.dropna(axis=0)

sector_etf.to_excel('sector_etf.xlsx', index=True)
