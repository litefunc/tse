
import pandas as pd
import numpy as np
import requests
from functools import partial
from json import loads as jsonLoadsF
import cytoolz.curried
import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
import craw.crawler_fp1 as crawler
import craw.craw_tse as craw_tse
import datetime as dt
import astype as ast

s = requests.Session()


@cytoolz.curry
def gen_url(type: str, input_date: str) -> str:
    return 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={}&type={}'.format(input_date, type)


def get_plain_text(url: str) -> str:
    return crawler.session_get_text(s, url, {})


def get_dict(date: str) -> dict:
    return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)
# get_dict = toolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype, inputDateF)


###----每日收盤行情(全部(不含權證、牛熊證))----
# gen_url_giventype = partial(gen_url, 'ALLBUT0999')
def gen_url_giventype(input_date: str) -> str:
    return gen_url('ALLBUT0999', input_date)


lastdate = craw_tse.last_datetime('每日收盤行情(全部(不含權證、牛熊證))')
# def inputDateF(t: int) -> str:
#     return crawler.inputDateF(lastdate, t)
# inputDateF = partial(crawler.inputDateF, lastdate)


@cytoolz.curry
def craw_close(date: str) -> pd.DataFrame:
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
    return craw_tse.saveToSqliteF('每日收盤行情(全部(不含權證、牛熊證))', df)
# save = partial(craw_tse.saveToSqliteF, '每日收盤行情(全部(不含權證、牛熊證))')


def craw_save(date: str) -> None:
    return crawler.craw_save(craw_close, save, date)
# craw_save = partial(crawler.craw_save, craw_close, save)


# nPeriods = crawler.timeDeltaF(lastdate).days
nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
# generatorG = crawler.looper(craw_save, nPeriods)
# next(generatorG)
# for _ in generatorG:
#     pass
crawler.loop(craw_save, nPeriods)


###----大盤統計資訊----
lastdate = craw_tse.last_datetime('大盤統計資訊')


def craw_market(date: str) -> pd.DataFrame:
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
    return craw_tse.saveToSqliteF('大盤統計資訊', df)


def craw_save(date: str) -> None:
    return crawler.craw_save(craw_market, save, date)

nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
# generatorG = crawler.looper(craw_save, nPeriods)
# for _ in generatorG:
#     pass
crawler.loop(craw_save, nPeriods)


###----大盤統計資訊(報酬指數)----
lastdate = craw_tse.last_datetime('大盤統計資訊')


def craw_marketReturn(date: str) -> pd.DataFrame:
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
    return craw_tse.saveToSqliteF('大盤統計資訊', df)


def craw_save(date: str) -> None:
    return crawler.craw_save(craw_marketReturn, save, date)


nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
# generatorG = crawler.looper(craw_save, nPeriods)
# for _ in generatorG:
#     pass
crawler.loop(craw_save, nPeriods)


###----大盤成交統計----
lastdate = craw_tse.last_datetime('大盤成交統計')


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
    return craw_tse.saveToSqliteF('大盤成交統計', df)


def craw_save(date: str) -> None:
    return crawler.craw_save(craw, save, date)


nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
# generatorG = crawler.looper(craw_save, nPeriods)
# for _ in generatorG:
#     pass
crawler.loop(craw_save, nPeriods)


###----漲跌證券數合計----
lastdate = craw_tse.last_datetime('漲跌證券數合計')


def craw_upsAndDown(date: str) -> pd.DataFrame:
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
    return craw_tse.saveToSqliteF('漲跌證券數合計', df)


def craw_save(date: str) -> None:
    return crawler.craw_save(craw_upsAndDown, save, date)


nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
# generatorG = crawler.looper(craw_save, nPeriods)
# for _ in generatorG:
#     pass
crawler.loop(craw_save, nPeriods)


###----牛證(不含可展延牛證)----
gen_url_giventype = partial(gen_url, '0999C')
lastdate = craw_tse.last_datetime('牛證(不含可展延牛證)')


def craw_callableBull(date: str) -> pd.DataFrame:
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
    return craw_tse.saveToSqliteF('牛證(不含可展延牛證)', df)


def craw_save(date: str) -> None:
    return crawler.craw_save(craw_callableBull, save, date)


nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
# generatorG = crawler.looper(craw_save, nPeriods)
# for _ in generatorG:
#     pass
crawler.loop(craw_save, nPeriods)


###----熊證(不含可展延熊證)----
gen_url_giventype = partial(gen_url, '0999B')
lastdate = craw_tse.last_datetime('熊證(不含可展延熊證)')


def craw_callableBear(date: str) -> pd.DataFrame:
    d = get_dict(date)
    if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
        raise crawler.NoData('很抱歉，沒有符合條件的資料!')
    data = d['data1']
    fields = d['fields1']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>", 1).replace("<p style= color:green>-</p>", -1).replace('X', 0).replace(' ', 0)
    df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('', 0).replace('＊', 1).replace('*', 1).fillna(np.nan)
    df['本益比'] = df['本益比'].replace('', np.nan).fillna(np.nan)
    intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
    floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格','標的證券收盤價/指數']
    df = ast.to_int(intColumns, df)
    df = ast.to_float(floatColumns, df)
    return df


def save(df: pd.DataFrame) -> None:
    return craw_tse.saveToSqliteF('熊證(不含可展延熊證)', df)


def craw_save(date: str) -> None:
    return crawler.craw_save(craw_callableBear, save, date)


nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
# generatorG = crawler.looper(craw_save, nPeriods)
# for _ in generatorG:
#     pass
crawler.loop(craw_save, nPeriods)


###----可展延牛證----
gen_url_giventype = partial(gen_url, '0999X')
lastdate = craw_tse.last_datetime('可展延牛證')


def craw_extendedCallableBear(date: str) -> pd.DataFrame:
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
    return craw_tse.saveToSqliteF('可展延牛證', df)


def craw_save(date: str) -> None:
    return crawler.craw_save(craw_extendedCallableBear, save, date)


nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
# generatorG = crawler.looper(craw_save, nPeriods)
# for _ in generatorG:
#     pass
crawler.loop(craw_save, nPeriods)

s.close()
