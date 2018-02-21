"""
This module contains the attributes necessary to
interact with a MS SQL Server database and its data
"""

from sqlalchemy import create_engine
from urllib import parse
from sqlalchemy.orm import sessionmaker


class MSSqlHandle:

    def __init__(self, driver_name, host_name,
                 db_name, trusted_connection='yes', uid=None,
                 pwd=None):
        """
        Handler to an MS SQL Server database and its contents
        Initializes SQLAlchemy engine, using pyodbc as the DBAPI and the connection dictionary provided

        Parameters
        ----------
        driver_name :
        host_name :
        db_name :
        trusted_connection :
        uid :
        pwd :

        """
        self.driver = driver_name
        self.server = host_name
        self.database = db_name
        self.trusted_conn = trusted_connection
        self.uid = uid
        self.pwd = pwd
        self.engine = create_engine("mssql+pyodbc:///?odbc_connect={0}".format(self._to_url()))
        self.Session = sessionmaker(bind=self.engine)

        # Check that the database connection works
        # during instantiation before getting a
        # false positive, i.e. the object is created
        try:
            self.engine.connect()
        except:
            raise


    def _to_url(self):
        """
        Used to properly format the connection URL

        Returns
        -------

        """
        connection_string = ("DRIVER={0};"
                             "SERVER={1};"
                             "DATABASE={2};"
                             "Trusted_Connection={3}".
                             format(self.driver, self.server,
                                    self.database, self.trusted_conn))
        connection_url = parse.quote_plus(connection_string)

        return connection_url


    def bulk_insert(self, csv_file_path, sql_schema_and_table,
                    field_terminator=',', row_terminator='\\n'):
        """

        Parameters
        ----------
        csv_file_path :
        sql_schema_and_table :
        field_terminator :
        row_terminator :

        Returns
        -------

        """
        sql = ("BULK INSERT {0}\n"
               "FROM '{1}'\n"
               "WITH (FIRSTROW=2, FIELDTERMINATOR = '{2}', ROWTERMINATOR = '{3}');".format(
                   sql_schema_and_table,
                   csv_file_path,
                   field_terminator,
                   row_terminator
               ))
        # This is the only implementation that works at the moment in
        # issuing and committing the BULK INSERT functionality successfully
        # in SQL Server (2016)
        raw_con = self.engine.raw_connection()
        cursor = raw_con.cursor()
        cursor.execute(sql)
        cursor.commit()
        cursor.close()


if __name__ == '__main__':
    pass
