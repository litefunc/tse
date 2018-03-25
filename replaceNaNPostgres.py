import pandas as pd
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

from common.connection import conn_local_pg

listTbSql = '''SELECT table_schema,table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE' AND table_schema = 'public'
ORDER BY table_schema,table_name;'''

for db in ['mops', 'tse', 'summary']:
    conn = conn_local_pg(db)
    cur = conn.cursor()
    cur.execute(listTbSql)
    tbs = [t[1] for t in cur.fetchall()]
    for t in tbs:
        sql = '''select * from "{}" LIMIT 1;'''.format(t)
        cols = list(pd.read_sql_query(sql, conn))
        for col in cols:
            try:
                print(t, col)
                sql = '''update "{0}" set "{1}" = null where "{1}" ='NaN';'''.format(t, col)
                cur.execute(sql)
            except Exception as e:
                print(e)
                pass
            conn.commit()
