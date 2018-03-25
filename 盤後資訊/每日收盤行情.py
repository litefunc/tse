import pandas as pd
import numpy as np
import requests
from functools import partial
from json import loads as jsonLoadsF
from typing import Generator
import cytoolz.curried
import datetime as dt

import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

import craw.crawler as crawler
import crawler.finance.tse.save as saver
import astype as ast


s = requests.Session()


@cytoolz.curry
def gen_url(type: str, input_date: str) -> str:
    return 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={}&type={}'.format(input_date, type)


def get_plain_text(url: str) -> str:
    return crawler.session_get_text(s, url, {})


def gen_url_giventype(input_date: str) -> str:
    return gen_url('ALLBUT0999', input_date)


def get_dict(date: str) -> dict:
    return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)

###----每日收盤行情(全部(不含權證、牛熊證))----

def crawler_close(table: str) -> Generator:
    def craw(date: str) -> pd.DataFrame:
        d = get_dict(date)
        if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
            raise crawler.NoData('很抱歉，沒有符合條件的資料!')
        data = d['data5']
        fields = d['fields5']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
        df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan).replace('', np.nan)
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace('<p style= color:red>+</p>', 1).replace('<p style= color:green>-</p>', -1).replace('X', 0).replace(' ', 0)
        df.insert(0, '年月日', date)
        df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
        floatColumns = ['成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比']
        df = ast.to_float(floatColumns, df)
        return df

    def save(df: pd.DataFrame) -> None:
        saver.lite(table, df)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = saver.last_datetime(table)
    nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
    return crawler.looper(craw_save, nPeriods)

for _ in crawler_close('每日收盤行情(全部(不含權證、牛熊證))'):
    pass

###----大盤統計資訊----

def crawler_market(table: str) -> Generator:
    def craw(date: str) -> pd.DataFrame:
        d = get_dict(date)
        if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
            raise crawler.NoData('很抱歉，沒有符合條件的資料!')
        data = d['data1']
        fields = d['fields1']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
        df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan).replace('---', np.nan)
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style ='color:red'>+</p>", 1).replace("<p style ='color:green'>-</p>", -1).replace('X', 0).replace(' ', 0)
        df.insert(0, '年月日', date)
        df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
        floatColumns = ['收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
        df = ast.to_float(floatColumns, df)
        return df

    def save(df: pd.DataFrame) -> None:
        saver.lite(table, df)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = saver.last_datetime(table)
    nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
    return crawler.looper(craw_save, nPeriods)

for _ in crawler_market('大盤統計資訊'):
    pass

###----大盤統計資訊(報酬指數)----

def crawler_marketReturn(table: str) -> Generator:
    def craw(date: str) -> pd.DataFrame:
        d = get_dict(date)
        if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
            raise crawler.NoData('很抱歉，沒有符合條件的資料!')
        data = d['data2']
        fields = d['fields2']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
        df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan)
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style ='color:red'>+</p>", 1).replace("<p style ='color:green'>-</p>", -1).replace('X', 0).replace(' ', 0)
        df.insert(0, '年月日', date)
        df = df.rename(columns={'報酬指數': '指數'})
        df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
        floatColumns = ['收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
        df = ast.to_float(floatColumns, df)
        return df

    def save(df: pd.DataFrame) -> None:
        saver.lite(table, df)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = saver.last_datetime(table)
    nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
    return crawler.looper(craw_save, nPeriods)

for _ in crawler_marketReturn('大盤統計資訊'):
    pass

###----大盤成交統計----

def crawler_composite(table: str) -> Generator:
    def craw(date: str) -> pd.DataFrame:
        d = get_dict(date)
        if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
            raise crawler.NoData('很抱歉，沒有符合條件的資料!')
        data = d['data3']
        fields = d['fields3']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
        df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan)
        df.insert(0, '年月日', date)
        df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
        floatColumns = ['成交金額(元)', '成交股數(股)', '成交筆數']
        df = ast.to_float(floatColumns, df)
        return df

    def save(df: pd.DataFrame) -> None:
        saver.lite(table, df)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = saver.last_datetime(table)
    nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
    return crawler.looper(craw_save, nPeriods)

for _ in crawler_composite('大盤成交統計'):
    pass

###----漲跌證券數合計----

def crawler_upsAndDown(table: str) -> Generator:
    def craw(date: str) -> pd.DataFrame:
        d = get_dict(date)
        if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
            raise crawler.NoData('很抱歉，沒有符合條件的資料!')
        data = d['data4']
        fields = d['fields4']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
        data[0][1].split('(')[0]
        L = []
        l = data[0]
        L.append([i.split('(')[0] for i in l])
        L.append([i.split('(')[1].replace(')', '') for i in l])
        l = data[1]
        L.append([i.split('(')[0] for i in l])
        L.append([i.split('(')[1].replace(')', '') for i in l])
        L.append(data[2])
        L.append(data[3])
        L.append(data[4])
        df = pd.DataFrame(L, columns=fields).replace(',', '', regex=True).replace('--', np.nan)
        df.insert(0, '年月日', date)
        df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
        intColumns = ['整體市場', '股票']
        df = ast.to_int(intColumns, df)
        return df

    def save(df: pd.DataFrame) -> None:
        saver.lite(table, df)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = saver.last_datetime(table)
    nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
    return crawler.looper(craw_save, nPeriods)

for _ in crawler_upsAndDown('漲跌證券數合計'):
    pass

###----牛證(不含可展延牛證)----

def crawler_callableBull(table: str) -> Generator:
    gen_url_giventype = partial(gen_url, '0999C')
    
    # gen_url_giventype is local func, can not be used by global get_dict, so make sure to def get_dict locally
    def get_dict(date: str) -> dict:
        return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)
    
    def craw(date: str) -> pd.DataFrame:
        d = get_dict(date)
        if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
            raise crawler.NoData('很抱歉，沒有符合條件的資料!')
        data = d['data1']
        fields = d['fields1']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
        df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan)
        df.insert(0, '年月日', date)
        df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>", 1).replace("<p style= color:green>-</p>",-1).replace('X', 0).replace(' ', 0)
        df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('', np.nan).fillna(np.nan)
        df['本益比'] = df['本益比'].replace('', np.nan).fillna(np.nan)
        intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
        floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格', '標的證券收盤價/指數']
        df = ast.to_int(intColumns, df)
        df = ast.to_float(floatColumns, df)
        return df

    def save(df: pd.DataFrame) -> None:
        saver.lite(table, df)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = saver.last_datetime(table)
    nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
    return crawler.looper(craw_save, nPeriods)

for _ in crawler_callableBull('牛證(不含可展延牛證)'):
    pass

###----熊證(不含可展延熊證)----

def crawler_callableBear(table: str) -> Generator:
    gen_url_giventype = partial(gen_url, '0999B')
    
    # gen_url_giventype is local func, can not be used by global get_dict, so make sure to def get_dict locally
    def get_dict(date: str) -> dict:
        return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)
    
    def craw(date: str) -> pd.DataFrame:
        d = get_dict(date)
        if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
            raise crawler.NoData('很抱歉，沒有符合條件的資料!')
        data = d['data1']
        fields = d['fields1']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
        df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan)
        df.insert(0, '年月日', date)
        df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>", 1).replace("<p style= color:green>-</p>",-1).replace('X', 0).replace(' ', 0)
        df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('', np.nan).fillna(np.nan)
        df['本益比'] = df['本益比'].replace('', np.nan).fillna(np.nan)
        intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
        floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格', '標的證券收盤價/指數']
        df = ast.to_int(intColumns, df)
        df = ast.to_float(floatColumns, df)
        return df

    def save(df: pd.DataFrame) -> None:
        saver.lite(table, df)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = saver.last_datetime(table)
    nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
    return crawler.looper(craw_save, nPeriods)

for _ in crawler_callableBear('熊證(不含可展延熊證)'):
    pass

###----可展延牛證----

def crawler_extendedCallableBear(table: str) -> Generator:
    gen_url_giventype = partial(gen_url, '0999X')
    
    # gen_url_giventype is local func, can not be used by global get_dict, so make sure to def get_dict locally
    def get_dict(date: str) -> dict:
        return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)
    
    def craw(date: str) -> pd.DataFrame:
        d = get_dict(date)
        if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
            raise crawler.NoData('很抱歉，沒有符合條件的資料!')
        data = d['data1']
        fields = d['fields1']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
        df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan)
        df.insert(0, '年月日', date)
        df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>", 1).replace("<p style= color:green>-</p>", -1).replace('X', np.nan).replace(' ', 0)
        df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('', 0).replace('＊', 1).replace('*', 1).fillna(np.nan)
        df['本益比'] = df['本益比'].replace('', np.nan).fillna(np.nan)
        intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
        floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格',
                        '標的證券收盤價/指數']
        df = ast.to_int(intColumns, df)
        df = ast.to_float(floatColumns, df)
        return df

    def save(df: pd.DataFrame) -> None:
        saver.lite(table, df)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = saver.last_datetime(table)
    nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
    return crawler.looper(craw_save, nPeriods)

for _ in crawler_extendedCallableBear('可展延牛證'):
    pass

s.close()
