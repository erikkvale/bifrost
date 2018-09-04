import pandas
from sqlalchemy.exc import ProgrammingError
from pandas.io.sql import get_schema
from sqlalchemy import create_engine
from psycopg2 import sql, DataError
import io


def initialize_engine(conn_str):
    """
    Creates, tests, and returns
    a new SQLAlchemy engine object
    """
    engine = create_engine(conn_str)
    try:
        conn = engine.connect()
        conn.close()
    except Exception:
        raise
    return engine


def convert_dataframe(dataframe, *args, **kwargs):
    """
    Converts dataframe to an in-memory text
    object and returns said file-like object
    """
    csv_buffer = io.StringIO()
    dataframe.to_csv(csv_buffer, *args, **kwargs)
    csv_buffer.seek(0)
    return csv_buffer

def create_sql_table(dataframe, table_name, engine):
    schema = get_schema(dataframe, table_name)
    engine.execute(schema)
    return None

def build_sql_query(table_name):
    """
    Creates and returns the PostgreSQL-specific
    SQL query as a Composable type, to safely
    parametrize and format the query
    """
    table_composable = sql.Identifier(table_name)
    return sql.SQL(
        "COPY {} FROM STDIN WITH CSV HEADER;"
    ).format(table_composable)


def bulk_load(file, sql_query, engine):
    raw_con = engine.raw_connection()
    cursor = raw_con.cursor()
    cursor.copy_expert(
        file=file_obj,
        sql=sql_query
    )
    raw_con.commit()
    return None


if __name__ == '__main__':
    df = pandas.DataFrame(
        data={
            'col_1': [1, 2, 3],
            'col_2': [3, 4, 5]
        }
    )
    conn_str = "postgresql+psycopg2://postgres:Gunnar14@localhost:5432/test"
    sql_table_name = "testing"

    # Execution order
    engine = initialize_engine(conn_str)
    file_obj = convert_dataframe(df, index=False)
    sql_query = build_sql_query(sql_table_name)

    # Load data
    try:
        bulk_load(file_obj, sql_query, engine)
    except DataError:
        print(file_obj.getvalue())




