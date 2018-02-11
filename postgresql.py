"""
Connecting to PostgreSQL
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class PostgreSqlHandle:

    """
    Handler to a PostgreSQL database and its contents
    """

    def __init__(self, db_name, username,
                 password, hostname, port=5432):
        """
        Initializes SQLAlchemy engine, using psycopg2 as the DBAPI

        http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#dialect-postgresql-psycopg2-connect

        :param db_name: <str>
        :param username: <str>
        :param password: <str>
        :param hostname: <str>
        :param port: <int>
        """
        self.connection_string = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
            username,
            password,
            hostname,
            port,
            db_name
        )
        self.engine = create_engine(self.connection_string)
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


    def bulk_copy(self, sql_schema_and_table, csv_file_path,
                  local_to_remote=False):
        """
        Attempts to load a CSV file into a destination SQL table using the the current
        SQLAlchemy engine's raw_connection() method.

        :param sql_schema_and_table: <str> in the form
            <destination_schema_name>.<destination_table_name>
        :param csv_file_path: <str> path of csv file to load to SQL
        :param local_to_remote: <bool> Defaults to False, meaning the SQL database
            is on the system running this client code. Otherwise set to true, i.e. SQL
            database is being accessed remotely.

        https://www.postgresql.org/docs/9.2/static/sql-copy.html

        :return:
        """
        dbapi_conn = self.engine.raw_connection()
        cursor = dbapi_conn.cursor()

        try:
            if not local_to_remote:
                sql = ("COPY {0} FROM '{1}' WITH CSV HEADER".
                       format(sql_schema_and_table, csv_file_path))
                cursor.execute(sql)
                dbapi_conn.commit()
            else:
                sql = ("COPY {0} FROM STDIN WITH CSV HEADER".
                       format(sql_schema_and_table))
                with open(csv_file_path) as file:
                    cursor.copy_expert(sql, file)
                dbapi_conn.commit()
        except:
            raise Exception
        finally:
            cursor.close()




if __name__=='__main__':
    pass




