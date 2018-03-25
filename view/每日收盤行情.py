import pandas as pd
import psycopg2
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

import sqlCommand as sqlc
from common.connection import conn_local_pg


conn = conn_local_pg('tse')
cur = conn.cursor()

# ---- view ----
index = pd.read_sql_query('SELECT * FROM "{}"'.format('大盤統計資訊'), conn).drop(['漲跌(+/-)'], axis=1)
uniq = index['指數'].unique()

sqlc.execute_pg(cur, """CREATE extension if not exists tablefunc;""")

def try_catch(conn: sqlc.conn_pg, li: list) -> None:
    try:
        cur = conn.cursor()
        [sqlc.execute_pg(cur, sql) for sql in li]
    except Exception as e:
        print(e)
        conn.rollback()
    else:
        conn.commit()


sql1 = """DROP view "大盤統計資訊-收盤指數";"""
sql2 = """create view "大盤統計資訊-收盤指數" as (
SELECT * FROM crosstab('select "年月日", "指數", "收盤指數" from "大盤統計資訊" order by 1 desc,2') AS final_result("年月日" date, {})
)""".format(', '.join(['"{}" {}'.format(c, 'float4') for c in uniq]))
try_catch(conn, [sql1, sql2])

sql1 = """DROP view "大盤統計資訊-漲跌百分比";"""
sql2 = """create view "大盤統計資訊-漲跌百分比" as(
SELECT * FROM crosstab('select "年月日", "指數", "漲跌百分比(%)" from "大盤統計資訊" order by 1 desc,2') AS final_result("年月日" date, {})
)""".format(', '.join(['"{}" {}'.format(c, 'float4') for c in uniq]))
try_catch(conn, [sql1, sql2])

conn.close()
