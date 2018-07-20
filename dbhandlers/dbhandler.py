"""
The classes in this module are meant to be minimally engineered
as they are built on libraries that are well designed and well developed.


Background
------------
The need to engineer by slightly extending, or rather combining existing
functionality across multiple libraries, resulted from working with the
Pandas library and its DataFrame class, which specifies a to_sql() method.
There was a lot of variance in implementation of SQL inserts across multiple
SQL database flavors, which led to variance in speed, especially noticeable
inserting more than a few thousand records with the to_sql() method. This
seemed to be an issue for many other users when passing in a SQLAlchemy
Engine() built from an underlying DBAPI dialect for the target SQL database.
Workflows with DataFrame objects containing hundreds of thousands, if not
millions of rows/records were simply very slow to write to SQL.


To work around the issue and speed up the movement of large datasets, the
leveraging of the target SQL database's native "bulk" load or insert functionality
for CSV files seems to be the fastest method.
"""
import psycopg2
from sqlalchemy import create_engine


class PostgreSqlHandle:

    def __init__(self, dbname, user,
                 password, host, port=5432):

        # http://initd.org/psycopg/docs/connection.html#connection
        self.conn = psycopg2.connect(
            dbname = dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )


    def __repr__(self):
        return "PostgreSqlHandle({}, {}, {}, {}, {})".format(
            **self.conn.get_dsn_parameters()
        )

    def __str__(self):
        pass


if __name__=='__main__':
    pg_sql_handle = PostgreSqlHandle(
        dbname='test_pg',
        user='postgres',
        password='Gunnar14',
        host='localhost'
    )