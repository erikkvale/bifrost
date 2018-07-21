"""
DataFrame -> CSV file -> SQL table
SQL table -> CSV file -> DataFrame
"""
import os
import time
from tempfile import NamedTemporaryFile
import psycopg2
from psycopg2 import sql
import sqlalchemy
import pandas
import numpy


#================================
# To SQL
#================================
_conn_dict = {
    'dbname': 'test_pg',
    'user': 'postgres',
    'password': 'Gunnar14',
    'host': 'localhost',
    'port': 5432
}
conn_str = ('postgresql+psycopg2://'
            '{user}:'
            '{password}@'
            '{host}:'
            '{port}/'
            '{dbname}'.format(**_conn_dict))

engine = sqlalchemy.create_engine(conn_str, use_batch_mode=True)
dbapi_conn = engine.raw_connection()


def _to_sql(csv_file, sql_table, conn):
    cursor = conn.cursor()
    table_composable = sql.Identifier(sql_table)
    sql_query = (sql.SQL("COPY {} FROM STDIN WITH CSV HEADER;").
                 format(table_composable))
    with open(csv_file, mode='r') as f:
        cursor.copy_expert(sql=sql_query, file=f)
        conn.commit()
    return cursor.rowcount, cursor.statusmessage


def load_csv(df):
    try:
        with NamedTemporaryFile(delete=False, suffix='.csv') as temp_csv:
            temp_csv.close()
            df.to_csv(temp_csv.name)
            result = _to_sql(temp_csv.name, 'test_data', dbapi_conn)
    except:
        raise
    finally:
        os.unlink(temp_csv.name)
    return result





if __name__ == '__main__':
    df = pandas.DataFrame(
        data=numpy.random.randint(
            0, 100,
            size=(10005, 4)),
        columns=['A', 'B', 'C', 'D']
    )
    start = time.time()
    df.to_sql('test_data', engine, if_exists='append')
    end = time.time()
    print("DataFrame.to_sql() time: {}".format(end - start))

    print('-'*20)

    start = time.time()
    result = load_csv(df)
    end = time.time()
    elapsed_time = end - start
    print("psycopg2 copy_expert() time: {} Rows: {}".format(elapsed_time, result))

