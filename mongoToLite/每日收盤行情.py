import sys
import os
import syspath
import crawler.finance.tse.save as saver
import crawler.finance.tse.mongoToLite.transform.每日收盤行情 as daily
import craw.crawler as crawler
from crawler.finance.tse.tradingday import adjust
from crawler.finance.tse.tradingday.db import days_lite
from pymongo import MongoClient
import pymongo
import datetime as dt
import time
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

port = int(os.getenv('MONGO_DOCKER_PORT'))
user = os.getenv('MONGO_DOCKER_USER')
pwd = os.getenv('MONGO_DOCKER_PWD')
client = MongoClient('localhost', port, username=user, password=pwd)

db = client['tse']
# db.collection_names(include_system_collections=False)

coll = db['每日收盤行情']

# ----每日收盤行情(全部(不含權證、牛熊證))----


def mgo_close(coll) -> None:
    table = '每日收盤行情(全部(不含權證、牛熊證))'
    firstday = dt.datetime(2004, 2, 11)
    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) != None]
    
    fields = ['證券代號', '證券名稱', '成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比']
    for date in dates:
        doc = coll.find_one({"date": date})
        if 'tables' not in doc:
            for i in range(1, 10):
                field = f'fields{i}'
                data = f'data{i}'
                if field in doc:
                    if doc[field] == fields:
                        print(date, table)
                        daily.close(date, doc[field], doc[data])
        else:
            for i in range(0, 9):
                if doc['tables'][i]['fields'] == fields:
                    print(date, table)
                    daily.close(date, fields, doc['tables'][i]['data'])


# ----大盤統計資訊----
def mgo_market(coll) -> None:
    table = '大盤統計資訊'
    firstday = dt.datetime(2009, 1, 5)
    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) != None]
    
    fields = ['指數', '收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
    for date in dates:
        doc = coll.find_one({"date": date})
        if 'tables' not in doc:
            for i in range(1, 6):
                field = f'fields{i}'
                data = f'data{i}'
                if field in doc:
                    if doc[field] == fields:
                        print(date, table)
                        daily.market(date, doc[field], doc[data])
        else:
            for i in range(0, 9):
                if doc['tables'][i]['fields'] == fields:
                    print(date, table)
                    daily.market(date, fields, doc['tables'][i]['data'])                        


# ----大盤統計資訊(報酬指數)----
def mgo_marketReturn(coll) -> None:
    table = '大盤統計資訊'
    firstday = dt.datetime(2009, 1, 5)
    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) != None]
    
    fields = ['報酬指數', '收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
    for date in dates:
        doc = coll.find_one({"date": date})
        if 'tables' not in doc:
            for i in range(1, 6):
                field = f'fields{i}'
                data = f'data{i}'
                if field in doc:
                    if doc[field] == fields:
                        print(date, table)
                        daily.marketReturn(date, doc[field], doc[data])
        else:
            for i in range(0, 9):
                if doc['tables'][i]['fields'] == fields:
                    print(date, table)
                    daily.marketReturn(date, fields, doc['tables'][i]['data'])                             


# ----大盤成交統計----
def mgo_composite(coll) -> None:
    table = '大盤成交統計'
    firstday = dt.datetime(2004, 2, 11)
    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) != None]
    
    fields = ['成交統計', '成交金額(元)', '成交股數(股)', '成交筆數']
    for date in dates:
        doc = coll.find_one({"date": date})
        if 'tables' not in doc:
            for i in range(1, 6):
                field = f'fields{i}'
                data = f'data{i}'
                if field in doc:
                    if doc[field] == fields:
                        print(date, table)
                        daily.composite(date, doc[field], doc[data])
        else:
            for i in range(0, 9):
                if doc['tables'][i]['fields'] == fields:
                    print(date, table)
                    daily.composite(date, fields, doc['tables'][i]['data'])      


# ----漲跌證券數合計----
def mgo_upsAndDown(coll) -> None:
    table = '漲跌證券數合計'
    firstday = dt.datetime(2011, 8, 1)
    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) != None]
    
    fields = ['類型', '整體市場', '股票']
    for date in dates:
        doc = coll.find_one({"date": date})
        if 'tables' not in doc:
            for i in range(1, 6):
                field = f'fields{i}'
                data = f'data{i}'
                if field in doc:
                    if doc[field] == fields:
                        print(date, table)
                        daily.upsAndDown(date, doc[field], doc[data])
        else:
            for i in range(0, 9):
                if doc['tables'][i]['fields'] == fields:
                    print(date, table)
                    daily.upsAndDown(date, fields, doc['tables'][i]['data'])                             


# ----牛證(不含可展延牛證)----
def mgo_callableBull() -> None:
    table = '牛證(不含可展延牛證)'
    coll = client['tse'][table]
    firstday = dt.datetime(2011, 7, 8)
    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) != None]
    for date in dates:
        print(date, '牛證(不含可展延牛證)')
        doc = coll.find_one({"date": date})
        if 'fields1' in doc:
             daily.callableBull(date, doc['fields1'], doc['data1'])
        else:            
            daily.callableBull(date, doc['tables'][8]['fields'], doc['tables'][8]['data'])



# ----熊證(不含可展延熊證)----
def mgo_callableBear() -> None:
    table = '熊證(不含可展延熊證)'
    coll = client['tse'][table]
    firstday = dt.datetime(2011, 7, 8)
    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) != None]
    for date in dates:
        doc = coll.find_one({"date": date})
        if 'fields1' in doc:
             daily.callableBull(date, doc['fields1'], doc['data1'])
        else:            
            daily.callableBull(date, doc['tables'][8]['fields'], doc['tables'][8]['data'])




# ----可展延牛證----
def mgo_extendedCallableBear() -> None:
    table = '可展延牛證'
    coll = db[table]
    firstday = dt.datetime(2014, 7, 31)
    lastdate = crawler.dt_to_str([saver.last_datetime(table)])
    days_db = days_lite(table)
    nPeriods = lastdate + \
        crawler.dt_to_str(adjust.days_trade(firstday) - days_db)
    dates = [t.replace('-', '')
             for t in nPeriods if coll.find_one({"date": t}) != None]
    for date in dates:
        doc = coll.find_one({"date": date})
        if 'fields1' in doc:
             daily.callableBull(date, doc['fields1'], doc['data1'])
        else:            
            daily.callableBull(date, doc['tables'][8]['fields'], doc['tables'][8]['data'])


mgo_close(coll)
mgo_market(coll)
mgo_marketReturn(coll)
mgo_composite(coll)
mgo_upsAndDown(coll)
mgo_callableBull()
mgo_callableBear()
mgo_extendedCallableBear()

# for doc in coll.find():
#    date = doc['date']
#    for i in range(1, 6):
#        field = f'fields{i}'
#        data = f'data{i}'
#        if field in doc:
#            if doc[field] == ['指數', '收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']:
#                print(date, '大盤統計資訊')
#                time.sleep(1)
#                daily.market(date, doc[field], doc[data])
#
#            if doc[field] == ['報酬指數', '收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']:
#                print(date, '大盤統計資訊')
#                time.sleep(1)
#                daily.marketReturn(date, doc[field], doc[data])
#
#            if doc[field] == ['成交統計', '成交金額(元)', '成交股數(股)', '成交筆數']:
#                print(date, '大盤成交統計')
#                time.sleep(1)
#                daily.composite(date, doc[field], doc[data])
#
#            if doc[field] == ['類型', '整體市場', '股票']:
#                print(date, '漲跌證券數合計')
#                time.sleep(1)
#                daily.upsAndDown(date, doc[field], doc[data])
#
#            if doc[field] == ['證券代號', '證券名稱', '成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比']:
#                print(date, '每日收盤行情(全部(不含權證、牛熊證))')
#                time.sleep(1)
#                daily.close(date, doc[field], doc[data])
