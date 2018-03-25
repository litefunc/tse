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


s = requests.Session()

@toolz.curry
def gen_url(type: str, input_date: str) -> str:
    return 'http://www.twse.com.tw/fund/T86?response=json&date={}&selectType={}'.format(input_date, type)


def get_plain_text(url: str) -> str:
    return crawler.session_get_text(s, url, {})


def get_dict(date: str) -> dict:
    return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)


def gen_url_giventype(input_date: str) -> str:
    return gen_url('ALL', input_date)


# def urlF(type, input_date):
#     return 'http://www.twse.com.tw/fund/T86?response=json&date={}&selectType={}'.format(input_date, type)
#
#
# urlF_givenType = partial(urlF, 'ALL')
lastdate = saver.last_datetime('三大法人買賣超日報')
# inputDateF = partial(crawler.inputDateF, lastdate)
# getDictF = fp.compose(jsonLoadsF, crawler.getPlainTextF, urlF_givenType, inputDateF)


def craw_institutional(date: str) -> pd.DataFrame:
    d = get_dict(date)
    if 'stat' in d and d['stat'] == '很抱歉，沒有符合條件的資料!':
        raise crawler.NoData('很抱歉，沒有符合條件的資料!')
    data = d['data']
    fields = d['fields']
    fields = [s.replace('</br>', '') for s in fields]
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan).replace('</br>', '', regex=True)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    df['證券名稱'] = df['證券名稱'].str.strip()
    cols = list(df)
    varchar_cols = ['年月日', '證券代號', '證券名稱']
    float_cols = [col for col in cols if col not in varchar_cols]
    df[float_cols] = df[float_cols].astype(float)
    # if '自營商買進股數' in list(df):
    #     floatColumns = ['外資買進股數', '外資賣出股數', '外資買賣超股數', '投信買進股數', '投信賣出股數', '投信買賣超股數', '自營商買賣超股數', '自營商買進股數', '自營商賣出股數', '三大法人買賣超股數']
    #     df[floatColumns] = df[floatColumns].astype(float)
    # else:
    #     floatColumns = ['外資買進股數', '外資賣出股數', '外資買賣超股數', '投信買進股數', '投信賣出股數', '投信買賣超股數', '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)', '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買賣超股數(避險)', '三大法人買賣超股數']
    #     df[floatColumns] = df[floatColumns].astype(float)
    return df


def save(df: pd.DataFrame) -> None:
    saver.lite('三大法人買賣超日報', df)


def craw_save(date: str) -> None:
    crawler.craw_save(save, craw_institutional, date)


# save = partial(craw_tse.saveToSqliteF, '三大法人買賣超日報')
# crawAndSaveF = partial(crawler.crawAndSaveF, craw_institutional, save)

nPeriods = crawler.input_dates(lastdate, dt.datetime.now())
# nPeriods = crawler.timeDeltaF(lastdate).days

generatorG = crawler.looper(craw_save, nPeriods)
for _ in generatorG:
    pass

#crawler.loop(craw_save, nPeriods)

s.close()
