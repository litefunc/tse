import pandas as pd
import numpy as np
from json import loads as jsonLoadsF
import datetime as dt
import requests
import cytoolz.curried
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

import craw.crawler as crawler
import crawler.finance.tse.save as saver
from tse.tradingday import adjust
from tse.tradingday.db import days_lite

s = requests.Session()


@cytoolz.curry
def gen_url(type: str, input_date: str) -> str:
    return 'http://www.twse.com.tw/block/BFIAUU?response=json&date={}&selectType={}'.format(input_date, type)


@cytoolz.curry
def get_plain_text(url: str) -> str:
    return crawler.session_get_text(s, url, {})


def get_dict(date: str) -> dict:
    return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)


def gen_url_giventype(input_date: str) -> str:
    return gen_url('S', input_date)


###----鉅額交易日成交資訊----

#!!! not everyday day has huge deal, most of day there are no data

# -- 1 company in 1 day may have more than 1 transaction --
def addNumberF(df):
    df.第幾筆 = list(range(1,len(df.第幾筆)+1))
    return df


lastdate = saver.last_datetime('鉅額交易日成交資訊')


empty = []
def craw_hugeDeal(date: str) -> pd.DataFrame:
    global empty
    
    d = get_dict(date)
    if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
        raise crawler.NoData('很抱歉，沒有符合條件的資料!')
    data = d['data']
    fields = d['fields']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    if data== []:
        empty = empty + [date]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan).replace('', np.nan)
    df = df[df['證券代號'] != '總計']
    df.insert(0, '年月日', date)
    df.insert(len(list(df)), '第幾筆', 1)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    intColumns = ['第幾筆']
    floatColumns = []
    for col in ['成交價', '成交股數', '成交金額', '成交量']:
        if col in list(df):
            floatColumns.append(col)
    df[floatColumns] = df[floatColumns].astype(float)
    df[intColumns] = df[intColumns].astype(int)
    df = df.groupby(['年月日', '證券代號']).apply(addNumberF)
    return df


def save(df: pd.DataFrame) -> None:
    saver.lite('鉅額交易日成交資訊', df)


def craw_save(date: str) -> None:
    crawler.craw_save(save, craw_hugeDeal, date)


table = '鉅額交易日成交資訊'
#lastdate = crawler.dt_to_str([saver.last_datetime(table)])
#firstday = dt.datetime(2005, 4, 4)
#days_db = days_lite(table)
#nPeriods = lastdate + crawler.dt_to_str(adjust.days_trade(firstday) - days_db)

#
#ex = ['2005-04-04',
# '2005-04-06',
# '2005-04-07',
# '2005-04-08',
# '2005-04-12',
# '2005-04-13',
# '2005-04-14',
# '2005-04-15',
# '2005-04-18',
# '2005-04-19',
# '2005-04-20',
# '2005-04-22',
# '2005-04-28',
# '2005-04-29',
# '2005-05-03',
# '2005-05-04',
# '2005-05-05',
# '2005-05-06',
# '2005-05-13',
# '2005-05-17',
# '2005-05-19',
# '2005-05-25',
# '2005-05-27',
# '2005-05-31',
# '2005-06-02',
# '2005-06-06',
# '2005-06-07',
# '2005-06-09',
# '2005-06-13',
# '2005-06-14',
# '2005-06-15',
# '2005-06-21',
# '2005-06-22',
# '2005-07-11',
# '2005-07-12',
# '2005-07-14',
# '2005-07-15',
# '2005-07-22',
# '2005-07-28',
# '2005-08-01',
# '2005-08-02',
# '2005-08-03',
# '2005-08-04',
# '2005-08-08',
# '2005-08-09',
# '2005-08-10',
# '2005-08-11',
# '2005-08-12','2005-08-16',
# '2005-08-18',
# '2005-08-19',
# '2005-08-22',
# '2005-08-23',
# '2005-08-24',
# '2005-08-26',
# '2005-08-30',
# '2005-09-05',
# '2005-09-07',
# '2005-09-09',
# '2005-09-12',
# '2005-09-14',
# '2005-09-15',
# '2005-09-19',
# '2005-09-20',
# '2005-09-22',
# '2005-09-23',
# '2005-09-27',
# '2005-09-29',
# '2005-09-30',
# '2005-10-05']
#
#exclude = [i.replace('-','') for i in ex]
#nPeriods = [i for i in nPeriods if i not in  exclude]

nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
generatorG = crawler.looper(craw_save, nPeriods)
for _ in generatorG:
    pass
#crawler.loop(craw_save, nPeriods)

s.close()
