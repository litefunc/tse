import pandas as pd
import numpy as np
from json import loads as jsonLoadsF
import datetime as dt
import cytoolz.curried
import requests
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
    return 'http://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date={}&selectType={}'.format(input_date, type)


@cytoolz.curry
def get_plain_text(url: str) -> str:
    return crawler.session_get_text(s, url, {})


def get_dict(date: str) -> dict:
    return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)


def gen_url_giventype(input_date: str) -> str:
    return gen_url('ALL', input_date)


# lastdate = saver.last_datetime('個股日本益比、殖利率及股價淨值比')

##----- pe is '0.00' when pe < 0 -----

def craw_priceEarning(date: str) -> pd.DataFrame:
    d = get_dict(date)
    if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
        raise crawler.NoData('很抱歉，沒有符合條件的資料!')
    data = d['data']
    fields = d['fields']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True)
    df = df.replace('--', np.nan).replace('-', np.nan)
    df['證券代號'] = df['證券代號'].str.strip()
    df['證券名稱'] = df['證券名稱'].str.strip()
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['殖利率(%)', '本益比', '股價淨值比']
    df[floatColumns] = df[floatColumns].astype(float)
    columns = ['年月日', '證券代號', '證券名稱', '殖利率(%)', '股利年度', '本益比', '股價淨值比', '財報年/季']
    if '股利年度' and '財報年/季' in list(df):
        intColumns = ['股利年度']
        df[intColumns] = df[intColumns].astype(int)
        df[floatColumns] = df[floatColumns].astype(float)
        df.股利年度 = df.股利年度 + 1911
        df['財報年/季'] = (df['財報年/季'].str.split('/').str[0].astype(int) + 1911).astype(str) + '/' + df['財報年/季'].str.split('/').str[1]
        df = df[columns]
    elif '財報年/季' in list(df):
        df['股利年度'] = np.nan
        df['財報年/季'] = (df['財報年/季'].str.split('/').str[0].astype(int) + 1911).astype(str) + '/' + df['財報年/季'].str.split('/').str[1]
        df = df[columns]
    else:
        df['股利年度'] = np.nan
        df['財報年/季'] = np.nan
        df = df[columns]
    return df


def save(df: pd.DataFrame) -> None:
    saver.lite('個股日本益比、殖利率及股價淨值比', df)


def craw_save(date: str) -> None:
    crawler.craw_save(save, craw_priceEarning, date)


table = '個股日本益比、殖利率及股價淨值比'
lastdate = crawler.dt_to_str([saver.last_datetime(table)])
firstday = dt.datetime(2005, 9, 2)
days_db = days_lite(table)
nPeriods = lastdate + crawler.dt_to_str(adjust.days_trade(firstday) - days_db)

# nPeriods = crawler.input_dates(lastdate, dt.datetime.now())

generatorG = crawler.looper(craw_save, nPeriods)
for _ in generatorG:
    pass
#crawler.loop(craw_save, nPeriods)

s.close()
