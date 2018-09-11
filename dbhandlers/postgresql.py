from psycopg2 import sql
from loader import initialize_engine, DataFrameLoaderMixin


class PostgresDataFrameLoader(DataFrameLoaderMixin):

    def __init__(self, dataframe, sqlalchemy_engine,
                 sql_table, sql_schema, **csv_kwargs):
        super().__init__(dataframe, sqlalchemy_engine,
                 sql_table, sql_schema, **csv_kwargs)

    def bulk_copy(self):
        """
        Uses Postgres' copy_expert method to load a Python
        file object as a CSV.

        Note: The usage of the inherited mixins instance
        attributes and properties. Specifically the engine
        and csv_file_obj
        """
        sql_query = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER;").format(
            sql.Identifier(table_name)
        )
        raw_conn = self.engine.raw_connection()
        cursor = raw_conn.cursor()
        cursor.copy_expert(file=self.csv_file_obj, sql=sql_query)
        raw_conn.commit()
        return cursor.rowcount


if __name__ == '__main__':
    import pandas
    df = pandas.DataFrame(
        data={
            'col_a': [1, 2, 3, 4, 5],
            'col_b': [6, 7, 8, 9, 10]
        }
    )
    table_name = 'testing'
    conn_str = 'postgresql+psycopg2://postgres:Gunnar14@localhost/test'
    engine = initialize_engine(conn_str)
    loader = PostgresDataFrameLoader(df, engine, 'testing', 'public', index=False)
    rc = loader.bulk_copy()




