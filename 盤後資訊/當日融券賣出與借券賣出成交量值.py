
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
def gen_url(input_date: str) -> str:
    return 'http://www.twse.com.tw/exchangeReport/TWTASU?response=json&date={}'.format(input_date, type)


@toolz.curry
def get_plain_text(url: str) -> str:
    return crawler.session_get_text(s, url)


def get_dict(date: str) -> dict:
    return toolz.compose(jsonLoadsF, get_plain_text, gen_url)(date)


lastdate = craw_tse.last_datetime('當日融券賣出與借券賣出成交量值(元)')


def craw_margin(date: str) -> pd.DataFrame:
    d = get_dict(date)
    if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
        raise crawler.NoData('很抱歉，沒有符合條件的資料!')
    data = d['data']
    fields = d['fields']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan).replace('-', np.nan)
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
    return craw_tse.saveToSqliteF('當日融券賣出與借券賣出成交量值(元)', df)


def craw_save(date: str) -> None:
    return crawler.craw_save(craw_margin, save, date)


nPeriods = crawler.input_dates(lastdate, dt.datetime.now())

# generatorG = crawler.looper(craw_save, nPeriods)
#
# for _ in generatorG:
#     pass

crawler.loop(craw_save, nPeriods)

s.close()
