"""
This module contains the
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class PostgreSqlHandle:

    def __init__(self, db_name, username,
                 password, hostname, port=5432):
        """
        Initializes SQLAlchemy engine, using psycopg2 as the DBAPI
        http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#dialect-postgresql-psycopg2-connect

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
        """

        self.conn_str = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
            username,
            password,
            hostname,
            port,
            db_name
        )
        self.engine = create_engine(self.conn_str)
        self.Session = sessionmaker(bind=self.engine)

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


    def bulk_copy(self, csv_file_path, sql_schema,
                  sql_table, local_to_remote=False):
        """
        Attempts to load a CSV file into a destination SQL table
        using the PostgreSQL native COPY functionality, which is
        an optimized way of loading large data sets.
        https://www.postgresql.org/docs/current/static/sql-copy.html

        Notes
        -----
        Assumes the CSV file has a header and the destination SQL
        tables already exist, so the header of the CSV is skipped.

        Parameters
        ----------
        csv_file_path : str
            The file path for the CSV file to load
        sql_schema : str
            The SQL schema/namespace of the SQL table
            to load the CSV file into
        sql_table : str
            The name of the SQL table to load the CSV file
            into
        local_to_remote : bool
            Default False, uses the COPY method, if True,
            then uses copy_expert of pyscopg2 to alter
        Returns
        -------

        """

        dbapi_conn = self.engine.raw_connection()
        cursor = dbapi_conn.cursor()

        try:
            if not local_to_remote:
                sql = "COPY %s.%s FROM %s WITH CSV HEADER"
                cursor.execute(sql, (sql_schema, sql_table, csv_file_path))
                dbapi_conn.commit()
            else:
                sql = "COPY %s.%s FROM STDIN WITH CSV HEADER"
                with open(csv_file_path) as file:
                    cursor.copy_expert(sql, file)
                dbapi_conn.commit()
        except:
            raise Exception
        finally:
            cursor.close()
        return None




if __name__=='__main__':
    pass




