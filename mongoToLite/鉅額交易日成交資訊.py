from pymongo import MongoClient
import pymongo
import datetime as dt
import time
import craw.crawler as crawler
import crawler.finance.tse.mongoToLite.transform.鉅額交易日成交資訊 as daily
import crawler.finance.tse.save as saver
import syspath
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
.tradingday import adjust
.tradingday.db import days_lite
port = int(os.getenv('MONGO_DOCKER_PORT'))
user = os.getenv('MONGO_DOCKER_USER')
pwd = os.getenv('MONGO_DOCKER_PWD')
client = MongoClient('localhost', port, username=user, password=pwd)


# ----鉅額交易日成交資訊----
def mgo_hugeDeal() -> None:
    table = '鉅額交易日成交資訊'
    coll = client['tse'][table]
    firstday = dt.datetime(2005, 4, 4)
    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) != None]
    for date in dates:
        doc = coll.find_one({"date": date})
        daily.hugeDeal(date, doc['fields'], doc['data'])


mgo_hugeDeal()
