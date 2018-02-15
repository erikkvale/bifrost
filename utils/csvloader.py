"""
This module contains function(s) to assist in
loading files with a format with some kind of delimiter
into a SQL database
"""
import os
import tempfile



def load_csv(df, sql_db_handler, sql_schema_name,
             sql_table_name, local_to_remote, header=True,
             columns=None, write_index=False, delimiter=',',
             newline='\n'):
    """
    Attempts to load a Pandas dataframe object into a SQL
    database while generating a temporary CSV file and leveraging
    the target SQL provider's bulk load utility.

    Parameters
    ----------
    df :
    sql_db_handler :
    sql_schema_name :
    sql_table_name :
    local_to_remote :
    header :
    columns :
    write_index :
    delimiter :
    newline :

    Returns
    -------

    """

    #Build full table name
    sql_schema_and_table = "{}.{}".format(sql_schema_name, sql_table_name)

    # Get SQLAlchemy engine name
    sql_flavor = sql_db_handler.engine.name

    try:
        # Because Windows is weird...
        if os.name=='nt':
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_csv:
                temp_csv.close()
                df.to_csv(
                    path_or_buf=temp_csv.name,
                    index=write_index,
                    header=header,
                    sep=delimiter,
                    line_terminator=newline,
                    columns=columns
                )

                if sql_flavor == 'postgresql':
                    sql_db_handler.bulk_copy(
                        sql_schema_and_table=sql_schema_and_table,
                        csv_file_path=temp_csv.name,
                        local_to_remote=local_to_remote
                    )
                elif sql_flavor == 'mssql':
                    sql_db_handler.bulk_insert(
                        sql_schema_and_table=sql_schema_and_table,
                        csv_file_path=temp_csv.name,
                    )
                else:
                    raise Exception("Bulk loading methods have not been added"
                                    "for this dialect of SQL: {0}".format(sql_flavor))
        else:
            pass
    except:
        raise Exception
    finally:
        os.unlink(temp_csv.name)