import pandas
import io
from sqlalchemy import create_engine, MetaData
from psycopg2 import sql
from pandas.io.sql import get_schema


def convert_dataframe(dataframe, *args, **kwargs):
    """Return in-memory string object"""
    csv_buffer = io.StringIO()
    dataframe.to_csv(csv_buffer, *args, **kwargs)
    return csv_buffer


def initialize_engine(conn_str, *args, **kwargs):
    """Create, test, and return SQLAlchemy engine"""
    engine = create_engine(conn_str, *args, **kwargs)
    try:
        conn = engine.connect()
        conn.close()
    except Exception:
        raise
    return engine


def create_table(engine, dataframe, table_name, schema=None):
    """Creates SQL table and schema if it exists"""
    if not engine.has_table(engine, table_name, schema):
        sql_create = get_schema(dataframe, table_name)
        metadata = MetaData(engine)
        print("{} table created".format(table_name))
        return None
    else:
        print("{} table already exists".format(table_name))


def postgres_bulk_copy(engine, file_obj, table_name, schema=None):
    """Uses Postgres' copy_expert method to load file object as CSV"""
    sql_query = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER;")
    sql_query = sql_query.format(sql.Identifier(table_name))
    raw_conn = engine.raw_connection()
    cursor = raw_conn.cursor()
    cursor.copy_expert(
        file=file_obj,
        sql=sql_query
    )
    raw_conn.commit()
    return (
        cursor.rowcount,
        cursor.statusmessage
    )




if __name__ == '__main__':
    df = pandas.DataFrame(
        data={
            'col_a': [1, 2, 3, 4, 5],
            'col_b': [6, 7, 8, 9, 10]
        }
    )
    table_name = 'testing'
    conn_str = 'postgresql+psycopg2://postgres:Gunnar14@localhost/test'
    file_obj = convert_dataframe(df, index=False)
    engine = initialize_engine(conn_str)
    create_table(engine, df, table_name)
    rc, stat = postgres_bulk_copy(engine, file_obj, table_name)
    print(rc)
    print(stat)






