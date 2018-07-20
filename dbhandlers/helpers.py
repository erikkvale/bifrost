import psycopg2
import psycopg2.extensions as pg_ext
import sqlalchemy


def _build_psycop2_conn(**kwargs):
    """Returns psyopg2. connection() object"""
    conn = psycopg2.connect(**kwargs)
    if conn.status == pg_ext.STATUS_READY:
        return conn


def _build_sqlalchemy_engine(**kwargs):
    dsn_str = ('postgresql+psycopg2://'
               '{user}:'
               '{password}@'
               '{host}:'
               '{port}/'
               '{dbname}'.format(**kwargs))
    engine = sqlalchemy.create_engine(dsn_str)
    try:
        conn = engine.connect()
        conn.close()
    except:
        raise
    else:
        return engine




if __name__ == '__main__':
    CONN_DICT = {
        'dbname': 'test_pg',
        'user': 'postgres',
        'password': 'Gunnar14',
        'host': 'localhost',
        'port': 5432
    }
    pg_conn = _build_psycop2_conn(**CONN_DICT)
    engine = _build_sqlalchemy_engine(**CONN_DICT)
