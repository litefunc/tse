from typing import Set
import datetime as dt
import cytoolz.curried
from typing import Generator
from json import loads as jsonLoadsF
import requests
import numpy as np
import pandas as pd
import syspath
import craw.crawler as crawler
import crawler.finance.tse.save as saver
import astype as ast
from tse.tradingday import adjust
from tse.tradingday.db import days_lite
from pymongo import MongoClient
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))


port = int(os.getenv('MONGO_DOCKER_PORT'))
user = os.getenv('MONGO_DOCKER_USER')
pwd = os.getenv('MONGO_DOCKER_PWD')
client = MongoClient('localhost', port, username=user, password=pwd)
db = client['tse']
coll = db['每日收盤行情']

s = requests.Session()


@cytoolz.curry
def gen_url(type: str, input_date: str) -> str:
    return 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={}&type={}'.format(input_date, type)


def get_plain_text(url: str) -> str:
    return crawler.session_get_text(s, url, {})


def gen_url_giventype(input_date: str) -> str:
    return gen_url('ALLBUT0999', input_date)


def get_dict(date: str) -> dict:
    return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)


def crawler_close(coll, table, firstday) -> Generator:
    def craw(date: str) -> dict:
        return get_dict(date)

    def save(d: dict) -> None:
        print(coll.insert_one(d).inserted_id)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    # firstday = dt.datetime(2004, 2, 11)
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) == None]
    print('dates to craw:', dates)
    return crawler.looper(craw_save, dates)


# ----每日收盤行情(全部(不含權證、牛熊證))----
for _ in crawler_close(coll, '每日收盤行情(全部(不含權證、牛熊證))', dt.datetime(2004, 2, 11)):
    pass

# ----大盤統計資訊----
for _ in crawler_close(coll, '大盤統計資訊', dt.datetime(2009, 1, 5)):
    pass

# ----大盤統計資訊(報酬指數)----
for _ in crawler_close(coll, '大盤統計資訊', dt.datetime(2009, 1, 5)):
    pass

# ----大盤成交統計----
for _ in crawler_close(coll, '大盤成交統計', dt.datetime(2004, 2, 11)):
    pass

# ----漲跌證券數合計----
for _ in crawler_close(coll, '漲跌證券數合計', dt.datetime(2011, 8, 1)):
    pass


# ----牛證(不含可展延牛證)----

def crawler_callableBull(coll, table, firstday) -> Generator:
    def gen_url_giventype(input_date: str) -> str:
        return gen_url('0999C', input_date)

    # gen_url_giventype is local func, can not be used by global get_dict, so make sure to def get_dict locally
    def get_dict(date: str) -> dict:
        return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)

    def craw(date: str) -> dict:
        return get_dict(date)

    def save(d: dict) -> None:
        print(coll.insert_one(d).inserted_id)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    # firstday = dt.datetime(2004, 2, 11)
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) == None]
    print('dates', dates)
    return crawler.looper(craw_save, dates)


for _ in crawler_callableBull(db['牛證(不含可展延牛證)'], '牛證(不含可展延牛證)', dt.datetime(2011, 7, 8)):
    pass


# ----熊證(不含可展延熊證)----

def crawler_callableBear(coll, table, firstday) -> Generator:

    def gen_url_giventype(input_date: str) -> str:
        return gen_url('0999B', input_date)

    # gen_url_giventype is local func, can not be used by global get_dict, so make sure to def get_dict locally
    def get_dict(date: str) -> dict:
        return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)

    def craw(date: str) -> dict:
        return get_dict(date)

    def save(d: dict) -> None:
        print(coll.insert_one(d).inserted_id)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    # firstday = dt.datetime(2004, 2, 11)
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) == None]
    print('dates', dates)
    return crawler.looper(craw_save, dates)


for _ in crawler_callableBear(db['熊證(不含可展延熊證)'], '熊證(不含可展延熊證)', dt.datetime(2011, 7, 8)):
    pass


# ----可展延牛證----

def crawler_extendedCallableBear(coll, table, firstday) -> Generator:

    def gen_url_giventype(input_date: str) -> str:
        return gen_url('0999X', input_date)

    # gen_url_giventype is local func, can not be used by global get_dict, so make sure to def get_dict locally
    def get_dict(date: str) -> dict:
        return cytoolz.compose(jsonLoadsF, get_plain_text, gen_url_giventype)(date)

    def craw(date: str) -> dict:
        return get_dict(date)

    def save(d: dict) -> None:
        print(coll.insert_one(d).inserted_id)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    # firstday = dt.datetime(2004, 2, 11)
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) == None]
    print('dates', dates)
    return crawler.looper(craw_save, dates)


for _ in crawler_extendedCallableBear(db['可展延牛證'], '可展延牛證', dt.datetime(2014, 7, 31)):
    pass

s.close()
