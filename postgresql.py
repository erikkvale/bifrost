"""
This module contains the
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from psycopg2 import sql


class PostgreSqlHandle:

    def __init__(self, db_name, username,
                 password, hostname, port=5432):
        """
        Initializes SQLAlchemy engine, using psycopg2 as the DBAPI
        (http://docs.sqlalchemy.org/en/latest/dialects/postgresql.
        html#dialect-postgresql-psycopg2-connect)

        Parameters
        ----------
        db_name : str
            The target PostgreSQL database name
        username : str
            Username credential for the PostgreSQL database
        password : str
            Password credential for the PostgreSQL database
        hostname : str
            The PostgreSQL instance name, ex. 'localhost'
        port : int
            The port that the target PostgreSQL instance is
            running on, default is 5432

        Attributes
        ----------
        conn_str: str
            The psycopg2 connection string, formatted with
            the instance's connection params
        engine: SQLAlchemy engine
            Initialized with the psycopg2 formatted conn_str
        Session: SQLAlchemy Session() class
            A class created from the sessionmaker class factory,
            with the current engine bound, so new instances of
            the Session() class can be constructed if needed using
            the current connection.
            #http://docs.sqlalchemy.org/en/latest/orm/session_basics.html#session-basics
        """

        self.conn_str = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
            username,
            password,
            hostname,
            port,
            db_name
        )
        self.engine = create_engine(self.conn_str)

        # Check that the database connection works
        # during instantiation, before getting a
        # false positive, i.e. the object is created
        # This is a behavior called "lazy initialization"
        # http://docs.sqlalchemy.org/en/latest/core/engines.html
        try:
            conn = self.engine.connect()
            conn.close()
        except:
            raise

        # create a configured "Session" class
        self.Session = sessionmaker(bind=self.engine)

    def csv_bulk_copy(self, file, table, direction='to_sql'):
        """
        Attempts to load the CSV data leveraging the PostgreSQL
        native COPY functionality, which is an optimized way of loading
        large data sets. Relies on psycopg2's cursor.copy_expert()
        method for both local <--> local and  local <--> remote data
        flows.

        http://initd.org/psycopg/docs/cursor.html#cursor.copy_expert

        Notes
        -----
        Assumes the CSV file has a header both ways and
        the destination SQL tables already exist.

        Parameters
        ----------
        file : str

        table :
        direction :

        Returns
        -------

        """

        # Set the raw connection and create cursor
        # for underlying DB-API
        # http://docs.sqlalchemy.org/en/latest/core/connections.html#working-with-raw-dbapi-connections
        dbapi_conn = self.engine.raw_connection()
        cur = dbapi_conn.cursor()

        # Default mode, will re-assign explicitly depending on direction
        file_mode = None

        # Use psycopg2 sql module for correct parameter injection.
        table_composable = sql.Identifier(table)

        # Set sql query string and file mode, both depending on direction
        if direction == 'to_sql':
            query = (sql.SQL("COPY {} FROM STDIN WITH CSV HEADER;").
                     format(table_composable))
            file_mode = 'r'
        elif direction == 'to_csv':
            query = (sql.SQL("COPY {} TO STDOUT WITH CSV HEADER;").
                     format(table_composable))
            file_mode = 'w'
        else:
            raise ValueError(
                "{} is not a valid direction. Use"
                "either 'to_sql' or 'to_csv'".format(
                direction
            ))

        try:
            with open(file, mode=file_mode) as f:
                cur.copy_expert(sql=query, file=f)
                if direction == 'to_sql':
                    dbapi_conn.commit()
        except:
            raise
        finally:
            cur.close()


if __name__=='__main__':
    # Usage
    db_handle = PostgreSqlHandle(
        db_name='somedb',
        username='erik',
        password='password',
        hostname='localhost'
    )
    db_handle.csv_bulk_copy(
        file=r'C:\Users\eirik\Desktop\sammmy.csv',
        table='sam_3d',
        direction='to_sql'
    )



