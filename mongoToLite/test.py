import pymongo
from pymongo import MongoClient
client = MongoClient('localhost', 27018, username='mongo', password='maxpower')
db = client['tse']

import datetime
db.collection_names(include_system_collections=False)

coll = db['每日收盤行情']

doc = coll.find_one()
doc == None
doc['fields1']

for fi in ['fields1', 'fields2', 'fields3', 'fields4', 'fields5']:
    if fi in doc:
        print(doc[fi])
    
for fi in ['fields1', 'fields2', 'fields3', 'fields4', 'fields5']:
    if doc[fi]= ['指數', '收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']:
        table = '大盤統計資訊'
    if doc[fi]= ['報酬指數', '收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']:
        table = '大盤統計資訊'
    if doc[fi]= ['成交統計', '成交金額(元)', '成交股數(股)', '成交筆數']:
        table = '大盤成交統計'
    if doc[fi]= ['類型', '整體市場', '股票']:
        table = ''
    if doc[fi]= ['證券代號', '證券名稱', '成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比']:
        table = '每日收盤行情(全部(不含權證、牛熊證))'
        
type(coll)
