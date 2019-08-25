import astype as ast
import crawler.finance.tse.save as saver
import syspath
from typing import List
import pandas as pd
import numpy as np
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))


# ----每日收盤行情(全部(不含權證、牛熊證))----
def close(date: str, fields: List[str], data: List[List[str]]) -> None:

    date = date[0:4] + '-' + date[4:6] + '-' + date[6:]
    df = pd.DataFrame(data, columns=fields).replace(
        ',', '', regex=True).replace('--', np.nan).replace('', np.nan)
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace('<p style= color:red>+</p>',
                                          1).replace('<p style= color:green>-</p>', -1).replace('X', 0).replace(' ', 0)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價',
                    '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比']
    df = ast.to_float(floatColumns, df)

    saver.lite('每日收盤行情(全部(不含權證、牛熊證))', df)


# ----大盤統計資訊----
def market(date: str, fields: List[str], data: List[List[str]]) -> None:

    date = date[0:4] + '-' + date[4:6] + '-' + date[6:]
    df = pd.DataFrame(data, columns=fields).replace(
        ',', '', regex=True).replace('--', np.nan).replace('---', np.nan)
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style ='color:red'>+</p>", 1).replace(
        "<p style ='color:green'>-</p>", -1).replace('X', 0).replace(' ', 0).replace('', 0)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
    df = ast.to_float(floatColumns, df)

    saver.lite('大盤統計資訊', df)


# ----大盤統計資訊(報酬指數)----
def marketReturn(date: str, fields: List[str], data: List[List[str]]) -> None:

    date = date[0:4] + '-' + date[4:6] + '-' + date[6:]
    df = pd.DataFrame(data, columns=fields).replace(
        ',', '', regex=True).replace('--', np.nan).replace('---', np.nan)
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style ='color:red'>+</p>", 1).replace(
        "<p style ='color:green'>-</p>", -1).replace('X', 0).replace(' ', 0).replace('', 0)
    df.insert(0, '年月日', date)
    df = df.rename(columns={'報酬指數': '指數'})
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
    df = ast.to_float(floatColumns, df)

    saver.lite('大盤統計資訊', df)


# ----大盤成交統計----
def composite(date: str, fields: List[str], data: List[List[str]]) -> None:

    date = date[0:4] + '-' + date[4:6] + '-' + date[6:]
    df = pd.DataFrame(data, columns=fields).replace(
        ',', '', regex=True).replace('--', np.nan)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['成交金額(元)', '成交股數(股)', '成交筆數']
    df = ast.to_float(floatColumns, df)

    saver.lite('大盤成交統計', df)


# ----漲跌證券數合計----
def upsAndDown(date: str, fields: List[str], data: List[List[str]]) -> None:

    date = date[0:4] + '-' + date[4:6] + '-' + date[6:]
    data[0][1].split('(')[0]
    L = []
    l = data[0]
    L.append([i.split('(')[0] for i in l])
    L.append([i.split('(')[1].replace(')', '') for i in l])
    l = data[1]
    L.append([i.split('(')[0] for i in l])
    L.append([i.split('(')[1].replace(')', '') for i in l])
    L.append(data[2])
    L.append(data[3])
    L.append(data[4])
    df = pd.DataFrame(L, columns=fields).replace(
        ',', '', regex=True).replace('--', np.nan)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    intColumns = ['整體市場', '股票']
    df = ast.to_int(intColumns, df)

    saver.lite('漲跌證券數合計', df)


# ----牛證(不含可展延牛證)----
def callableBull(date: str, fields: List[str], data: List[List[str]]) -> None:

    date = date[0:4] + '-' + date[4:6] + '-' + date[6:]
    df = pd.DataFrame(data, columns=fields).replace(
        ',', '', regex=True).replace('--', np.nan)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>",
                                          1).replace("<p style= color:green>-</p>", -1).replace('X', 0).replace(' ', 0)
    df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('＊', 1).replace('', 0).fillna(0)
    df['本益比'] = df['本益比'].replace('', np.nan).fillna(np.nan)
    intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
    floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價',
                    '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格', '標的證券收盤價/指數']
    df = ast.to_int(intColumns, df)
    df = ast.to_float(floatColumns, df)

    saver.lite('牛證(不含可展延牛證)', df)


# ----熊證(不含可展延熊證)----
def callableBear(date: str, fields: List[str], data: List[List[str]]) -> None:

    date = date[0:4] + '-' + date[4:6] + '-' + date[6:]
    df = pd.DataFrame(data, columns=fields).replace(
        ',', '', regex=True).replace('--', np.nan)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>",
                                          1).replace("<p style= color:green>-</p>", -1).replace('X', 0).replace(' ', 0)
    df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('＊', 1).replace('', 0).fillna(0)
    df['本益比'] = df['本益比'].replace('', np.nan).fillna(np.nan)
    intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
    floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價',
                    '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格', '標的證券收盤價/指數']
    df = ast.to_int(intColumns, df)
    df = ast.to_float(floatColumns, df)

    saver.lite('熊證(不含可展延熊證)', df)


# ----可展延牛證----
def extendedCallableBear(date: str, fields: List[str], data: List[List[str]]) -> None:

    date = date[0:4] + '-' + date[4:6] + '-' + date[6:]
    df = pd.DataFrame(data, columns=fields).replace(
        ',', '', regex=True).replace('--', np.nan)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>",
                                          1).replace("<p style= color:green>-</p>", -1).replace('X', np.nan).replace(' ', 0)
    df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace(
        '', 0).replace('＊', 1).replace('*', 1).fillna(np.nan)
    df['本益比'] = df['本益比'].replace('', np.nan).fillna(np.nan)
    intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
    floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格',
                    '標的證券收盤價/指數']
    floatColumns = [col for col in floatColumns if col in list(df)]
    df[intColumns + floatColumns] = df[intColumns +
                                       floatColumns].replace('', 0).fillna(np.nan)
    df = ast.to_int(intColumns, df)
    df = ast.to_float(floatColumns, df)

    saver.lite('可展延牛證', df)
