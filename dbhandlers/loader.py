"""
A module containing some basic functionality
to convert and load Pandas DataFrames into SQL
databases.
"""
from sqlalchemy import create_engine


def initialize_engine(conn_str, *args, **kwargs):
    """Create, test, and return SQLAlchemy engine"""
    engine = create_engine(conn_str, *args, **kwargs)
    try:
        conn = engine.connect()
        conn.close()
    except Exception:
        raise
    return engine