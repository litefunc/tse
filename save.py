import pandas as pd
from cytoolz.curried import curry
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

import sqlCommand as sqlc
import craw.crawler as crawler
from common.connection import conn_local_lite, conn_local_pg, conn_local_my, conn_local_mgo

conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
# conn_my = conn_local_my('tse')
db_mgo = conn_local_mgo['tse']


@curry
def lite(table: str, df: pd.DataFrame) -> None:
    sqlc.i_lite(conn_lite, table, df)


@curry
def pg(table: str, df: pd.DataFrame) -> None:
    global dferr
    try:
        sqlc.i_pg(conn_pg, table, df)
    except Exception as e:
        dferr = df
        raise type(e)(e)

# @curry
# def my(table: str, df: pd.DataFrame) -> None:
#     sqlc.i_my(conn_my, table, df)


@curry
def mongo(table: str, df: pd.DataFrame) -> None:
    d = df.to_dict(orient='records')
    collection = db_mgo[table]
    collection.insert_many(d)


last_datetime = crawler.last_datetime(conn_lite)
