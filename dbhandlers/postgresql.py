import pandas
from sqlalchemy.exc import ProgrammingError
from pandas.io.sql import get_schema
from sqlalchemy import create_engine
from psycopg2 import sql, DataError
import io


class SqlLoader:
    """Loads a Pandas dataframe into a SQL database"""

    def __init__(self, conn_str, dataframe,
                 table_name, *csv_args, **csv_kwargs):
        self.conn_str = conn_str
        self.dataframe = dataframe
        self.table_name = table_name

    @property
    def engine(self):
        return self._initialize_engine(self.conn_str)

    @property
    def raw_connection(self):
        return self.engine.raw_connection()

    @property
    def file_obj(self):
        return self._convert_dataframe(self.dataframe)

    @property
    def sql_query(self):
        return self._compose_sql_query(self.table_name)

    def bulk_copy(self):
        cursor = self.raw_connection.cursor()
        cursor.copy_expert(
            file=self.file_obj,
            sql=self.sql_query
        )
        self.raw_connection.commit()
        return None


    def _initialize_engine(self, conn_str):
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


    def _convert_dataframe(self, dataframe, *args, **kwargs):
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

    def _compose_sql_query(self, table_name):
        """
        Creates and returns the PostgreSQL-specific
        SQL query as a Composable type, to safely
        parametrize and format the query
        """
        table_composable = sql.Identifier(table_name)
        return sql.SQL(
            "COPY {} FROM STDIN WITH CSV HEADER;"
        ).format(table_composable)

if __name__ == '__main__':
    df = pandas.DataFrame(
        data={
            'col_1': [1, 2, 3],
            'col_2': [3, 4, 5]
        }
    )
    conn_str = "postgresql+psycopg2://postgres:Gunnar14@localhost:5432/test"
    sql_table_name = "testing"




