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
def gen_url(input_date: str) -> str:
    return 'http://www.twse.com.tw/exchangeReport/TWTASU?response=json&date={}'.format(input_date, type)


@cytoolz.curry
def get_plain_text(url: str) -> str:
    return crawler.session_get_text(s, url, {})


def get_dict(date: str) -> dict:
    return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url)(date)


# lastdate = saver.last_datetime('當日融券賣出與借券賣出成交量值(元)')


def craw_margin(date: str) -> pd.DataFrame:
    d = get_dict(date)
    if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
        raise crawler.NoData('很抱歉，沒有符合條件的資料!')
    data = d['data']
    fields = d['fields']
    if fields[1] == '數量':
        fields[1] = '融券賣出成交數量'

    if fields[2] == '金額':
        fields[2] = '融券賣出成交金額'

    if fields[3] == '數量':
        fields[3] = '借券賣出成交數量'

    if fields[4] == '金額':
        fields[4] = '借券賣出成交金額'

    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(
        ',', '', regex=True).replace('--', np.nan).replace('-', np.nan)
    df = df[df.證券名稱 != '合計']
    df.insert(0, '證券代號', df['證券名稱'].str.split().str[0].str.strip())
    df['證券名稱'] = df['證券名稱'].str.split().str[1].str.strip()
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['融券賣出成交金額', '借券賣出成交金額']
    df[floatColumns] = df[floatColumns].astype(float)
    intColumns = ['融券賣出成交數量', '借券賣出成交數量']
    df[intColumns] = df[intColumns].astype(int)
    return df


def save(df: pd.DataFrame) -> None:
    saver.lite('當日融券賣出與借券賣出成交量值(元)', df)


def craw_save(date: str) -> None:
    crawler.craw_save(save, craw_margin, date)


table = '當日融券賣出與借券賣出成交量值(元)'
lastdate = crawler.dt_to_str([saver.last_datetime(table)])
firstday = dt.datetime(2008, 9, 26)
days_db = days_lite(table)
nPeriods = lastdate + crawler.dt_to_str(adjust.days_trade(firstday) - days_db)

# nPeriods = crawler.input_dates(lastdate, dt.datetime.now())

generatorG = crawler.looper(craw_save, nPeriods)
for _ in generatorG:
    pass

#crawler.loop(craw_save, nPeriods)

s.close()
