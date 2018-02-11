"""
Connecting to MS SQL Server
"""

from sqlalchemy import create_engine
from urllib import parse
from sqlalchemy.orm import sessionmaker



class MSSqlHandle:
    """
    Handler to an MS SQL Server database and its contents
    """

    def __init__(self, driver_name, host_name,
                 db_name, trusted_connection='yes', uid=None,
                 pwd=None):
        """
        Initializes SQLAlchemy engine, using pyodbc as the DBAPI and the connection dictionary provided

        :param driver_name: <str> Ex: 'SQL Server Native Client 11.0''
        :param host_name: <str> Ex: 'ORION\TARS' <host>\<SQL Server instance>
        :param db_name: <str>
        :param trusted_connection: <bool> Uses Windows Authentication if true
        :param uid: Not Implemented
        :param pwd: Not Implemented
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
        https://docs.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql

        EXAMPLE:

        MSSqlHandle.bulk_insert(self,
                    csv_file_path=r'C:\mycsv.csv',
                    sql_schema_name='dbo.mytable'
                    field_terminator=',',
                    row_terminator='\\n'
                    )
        ****Default skips header row****
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
