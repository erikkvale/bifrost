"""
A module containing some basic functionality
to convert and load Pandas DataFrames into SQL
databases.
"""
from sqlalchemy import create_engine
from pandas.io.sql import get_schema
import io


def initialize_engine(conn_str, *args, **kwargs):
    """Create, test, and return SQLAlchemy engine"""
    engine = create_engine(conn_str, *args, **kwargs)
    try:
        conn = engine.connect()
        conn.close()
    except Exception:
        raise
    return engine


class DataFrameLoaderMixin:
    """
    Fill this docstring on completion
    """
    def __init__(self, dataframe, engine, sql_table,
                 sql_schema, **csv_kwargs):
        """
        Parameters
        ----------
        dataframe: Pandas DataFrame
        engine: SQLAlchemy Engine
        sql_table: str
            The destination SQL table name
        sql_schema: str
            The destination SQL schema name
        csv_kwargs: object
            These are the arguments to be passed on to
            the Pandas.DataFrame().to_csv() method. Most
            common here is to set index=False, to prevent
            the Pandas index from being written to file.
        """
        self.dataframe = dataframe
        self.engine = engine
        self.sql_schema = sql_schema
        self.sql_table = sql_table
        self.csv_kwargs = csv_kwargs

        if not self._table_exists():
            self._create_table()

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

    def __repr__(self):
        return "<{klass} @{id:x} {attrs}>".format(
            klass=self.__class__.__name__,
            id=id(self),
            attrs="".join("{}={!r},".format(k, v) for k, v in self.__dict__.items()),
        )