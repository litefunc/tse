from json import loads as jsonLoadsF
from typing import Generator
import datetime as dt
import requests
import cytoolz.curried
import os
import sys
import craw.crawler as crawler
import crawler.finance.tse.save as saver
from tse.tradingday import adjust
from tse.tradingday.db import days_lite

from pymongo import MongoClient
port = int(os.getenv('MONGO_DOCKER_PORT'))
user = os.getenv('MONGO_DOCKER_USER')
pwd = os.getenv('MONGO_DOCKER_PWD')
client = MongoClient('localhost', port, username=user, password=pwd)
db = client['tse']
coll = db['鉅額交易日成交資訊']

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


# ----鉅額交易日成交資訊----

#!!! not everyday day has huge deal, most of day there are no data

def craw_hugeDeal(coll) -> Generator:
    table = '鉅額交易日成交資訊'

    def craw(date: str) -> dict:
        return get_dict(date)

    def save(d: dict) -> None:
        print(coll.insert_one(d).inserted_id)

    def craw_save(date: str) -> None:
        crawler.craw_save(save, craw, date)

    firstday = dt.datetime(2005, 4, 4)
    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    print('nPeriods', nPeriods)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) == None]
    print('dates', dates)
    return crawler.looper(craw_save, dates)


for _ in craw_hugeDeal(coll):
    pass

s.close()

#[t.replace('-', '') for t in nPeriods if coll.find_one({"date": t}) == None]
#coll.find_one({"date": nPeriods[-1]})
# len(nPeriods)

# craw('20050513')
# craw('20050517')
# craw('20050513')

# for date in dates:
#     print(date)
#     craw(date)
# coll.find_one({"date": '20050429'}) == None
# g = craw_hugeDeal(coll)
# next(g)


# for t in nPeriods:
#     if coll.find_one({"date": t}) == None:
#         print(t)
