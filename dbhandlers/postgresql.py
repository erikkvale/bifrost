import pandas
import io
from sqlalchemy import create_engine
from psycopg2 import sql
from pandas.io.sql import get_schema


def initialize_engine(conn_str, *args, **kwargs):
    """Create, test, and return SQLAlchemy engine"""
    engine = create_engine(conn_str, *args, **kwargs)
    try:
        conn = engine.connect()
        conn.close()
    except Exception:
        raise
    return engine


class DataframeLoader:
    """
    Converts Pandas DataFrame to in-memory
    CSV file and leverages PostgreSQL's bulk
    copy method to load file into a SQL database
    """
    def __init__(self, dataframe, sqlalchemy_engine,
                 sql_table, sql_schema, **csv_kwargs):
        self.dataframe = dataframe
        self.engine = sqlalchemy_engine
        self.sql_schema = sql_schema
        self.sql_table = sql_table
        self.csv_kwargs = csv_kwargs

    @property
    def csv_file_obj(self):
        return self.convert_dataframe()

    def convert_dataframe(self):
        """Return in-memory string object"""
        csv_buffer = io.StringIO()
        self.dataframe.to_csv(csv_buffer, **self.csv_kwargs)
        csv_buffer.seek(0)
        return csv_buffer

    def _table_exists(self):
        """Inspects the database to see if the table exists"""
        if self.engine.dialect.has_table(
                self.engine, self.sql_table, schema=self.sql_schema):
            return True
        else:
            return False

    def _create_table(self):
        """Creates the SQL table using the schema of the current DataFrame object"""
        dataframe_schema = get_schema(self.dataframe, self.sql_table)
        with self.engine.connect() as conn:
            conn.execute(dataframe_schema)

    def bulk_copy(self):
        """Uses Postgres' copy_expert method to load file object as CSV"""
        if not self._table_exists():
            self._create_table()

        sql_query = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER;").format(
            sql.Identifier(table_name)
        )
        raw_conn = self.engine.raw_connection()
        cursor = raw_conn.cursor()
        cursor.copy_expert(file=self.csv_file_obj, sql=sql_query)
        raw_conn.commit()
        return cursor.rowcount




if __name__ == '__main__':
    df = pandas.DataFrame(
        data={
            'col_a': [1, 2, 3, 4, 5],
            'col_b': [6, 7, 8, 9, 10]
        }
    )
    table_name = 'testing'
    conn_str = 'postgresql+psycopg2://postgres:Gunnar14@localhost/test'
    engine = initialize_engine(conn_str)
    loader = DataframeLoader(df, engine, table_name, 'public', index=False)
    loader.bulk_copy()






