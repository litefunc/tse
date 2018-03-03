import pandas as pd
import numpy as np
from json import loads as jsonLoadsF
import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
import craw.crawler_fp1 as crawler
import craw.craw_tse as craw_tse

import datetime as dt
import toolz
import requests

s = requests.Session()


@toolz.curry
def gen_url(type: str, input_date: str) -> str:
    return 'http://www.twse.com.tw/block/BFIAUU?response=json&date={}&selectType={}'.format(input_date, type)


@toolz.curry
def get_plain_text(url: str) -> str:
    return crawler.session_get_text(s, url)


def get_dict(date: str) -> dict:
    return toolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)


def gen_url_giventype(input_date: str) -> str:
    return gen_url('S', input_date)


###----鉅額交易日成交資訊----


# -- 1 company in 1 day may have more than 1 transaction --
def addNumberF(df):
    df.第幾筆 = list(range(1,len(df.第幾筆)+1))
    return df


lastdate = craw_tse.last_datetime('鉅額交易日成交資訊')


def craw_hugeDeal(date: str) -> pd.DataFrame:
    d = get_dict(date)
    if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
        raise crawler.NoData('很抱歉，沒有符合條件的資料!')
    data = d['data']
    fields = d['fields']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
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
    return craw_tse.saveToSqliteF('鉅額交易日成交資訊', df)


def craw_save(date: str) -> None:
    return crawler.craw_save(craw_hugeDeal, save, date)


nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
# generatorG = crawler.looper(craw_save, nPeriods)
# for _ in generatorG:
#     pass
crawler.loop(craw_save, nPeriods)

s.close()
