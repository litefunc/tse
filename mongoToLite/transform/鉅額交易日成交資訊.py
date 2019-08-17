from typing import List
import pandas as pd
import numpy as np
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
import crawler.finance.tse.save as saver
import astype as ast

#!!! not everyday day has huge deal, most of day there are no data

# -- 1 company in 1 day may have more than 1 transaction --
def addNumberF(df):
    df.第幾筆 = list(range(1,len(df.第幾筆)+1))
    return df


###----鉅額交易日成交資訊----
def hugeDeal(date: str, fields: List[str], data: List[List[str]]) -> None:

    date = date[0:4] + '-' + date[4:6] + '-' + date[6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', np.nan).replace('', np.nan)
    df = df[df['證券代號'] != '總計']
    df.insert(0, '年月日', date)
    df.insert(len(list(df)), '第幾筆', 1)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    intColumns = ['第幾筆']
    floatColumns = []
    for col in ['成交價', '成交股數', '成交金額', '成交量']:
        if col in list(df):
            floatColumns.append(col)
    df[floatColumns] = df[floatColumns].astype(float)
    df[intColumns] = df[intColumns].astype(int)
    df = df.groupby(['年月日', '證券代號']).apply(addNumberF)

    saver.lite('鉅額交易日成交資訊', df)