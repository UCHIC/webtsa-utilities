import pandas as pd
import psycopg2
import os
import sys

try:
    from urllib import quote_plus
except ImportError:
    from urllib.parse import quote_plus

from Common import APP_SETTINGS
import sqlalchemy
from SqlSnippets import *


def build_connection_string(engine, host, database, username, password, port):
    if engine == 'mssql' and sys.platform != 'win32':
        quoted = quote_plus('DRIVER={FreeTDS};DSN=%s;UID=%s;PWD=%s;' % (host, username, password))
        conn_string = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    elif engine == 'mssql':
        driver = 'pyodbc'
        conn = '%s+%s://%s:%s@%s/%s?driver=SQL+Server'
        if 'sqlncli11.dll' in os.listdir('C:\\Windows\\System32'):
            conn = '%s+%s://%s:%s@%s/%s?driver=SQL+Server+Native+Client+11.0'
        conn_string = conn % (engine, driver, username, password, host, database)
    else:
        if engine == 'mysql':
            driver = 'pymysql'
        elif engine == 'postgresql':
            driver = 'psycopg2'
        else:
            driver = 'None'
        conn = '%s+%s://%s:%s@%s:%s/%s'
        conn_string = conn % (engine, driver, username, password, host, port, database)
    return conn_string


def fetch_catalog(connection_string, sql_snippets):
    values = pd.read_sql(sqlalchemy.text(sql_snippets.fetch_dataseries), connection_string)
    # if APP_SETTINGS.VERBOSE:
    #     print(values)
    return values

    # empty table and reset sequence counter
def purge_catalog(connection_string, sql_snippets):
    to_conn = sqlalchemy.create_engine(connection_string)
    conn = to_conn.connect()
    conn.execute(sql_snippets.purge_catalog)
    conn.execute(sql_snippets.reset_sequence)

    # fill table
def insert_into_catalog(connection_string, values):
    to_conn = sqlalchemy.create_engine(connection_string)
    values.to_sql(name="DataSeries", con=to_conn, if_exists="append", index=False)


if __name__ == "__main__":
    if update_catalog():
        print('Catalog updated without error')
        # import timeit
        # time = timeit.Timer(timer=update_catalog())
        # print(str(time))
