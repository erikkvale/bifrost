import pandas
import io
from sqlalchemy import create_engine
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


def postgres_bulk_copy(engine, file_object, table_name, schema=None):
    """Uses Postgres' copy_expert method to load file object as CSV"""
    pass


def _create_table(engine, dataframe, table_name, schema=None):
    if not engine.dialect.has_table(engine, table_name, schema):
        schema = get_schema(dataframe, table_name)
        engine.execute(schema)
        return None






