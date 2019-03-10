import pandas as pd
import numpy as np
import requests
from functools import partial
from json import loads as jsonLoadsF
from typing import Generator
from cytoolz.curried import curry
import datetime as dt
from typing import Set
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

import sqlCommand as sqlc
import craw.crawler as crawler
import crawler.finance.tse.save as saver
import astype as ast
from common.connection import conn_local_lite, conn_local_pg
from tse.tradingday import adjust

conn_lite = conn_local_lite('tse.sqlite3')


@curry
def __days_lite(conn_lite, col: str, table: str) -> Set[dt.datetime]:
    ser = pd.to_datetime(sqlc.s_dist_lite(conn_lite, table, [col])[col])
    return {x.to_pydatetime() for x in ser}


def days_lite(table: str) -> Set[dt.datetime]:
    return __days_lite(conn_lite, '年月日', table)