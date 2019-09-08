# ---TSE台灣證交所爬蟲(含下列資料庫):---

# 每日收盤行情(全部(不含權證、牛熊證))
# 大盤統計資訊
# 大盤統計資訊
# 大盤成交統計
# 漲跌證券數合計
# 牛證(不含可展延牛證)
# 熊證(不含可展延熊證)
# 可展延牛證
# 三大法人買賣超日報
# 個股日本益比、殖利率及股價淨值比
# 當日融券賣出與借券賣出成交量值(元)
# 鉅額交易日成交資訊


import crawler.finance.tse.鉅額交易.鉅額交易日成交資訊
import crawler.finance.tse.盤後資訊.當日融券賣出與借券賣出成交量值
import crawler.finance.tse.三大法人.三大法人買賣超日報
import crawler.finance.tse.盤後資訊.個股日本益比殖利率及股價淨值比
import crawler.finance.tse.盤後資訊.每日收盤行情1

import crawler.finance.tse.mongoToLite.鉅額交易日成交資訊1
import crawler.finance.tse.mongoToLite.每日收盤行情

from crawler.finance.sqliteToPostgres.update import *
