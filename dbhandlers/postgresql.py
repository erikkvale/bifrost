from interface import DbHandler
from collections import OrderedDict


_CONN_STR = "postgresql://{user}:{password}@{host}:{port}/{dbname}"
postgres_creds = OrderedDict({
    'dbname': '',
    'user': '',
    'password': '',
    'host': 'localhost',
    'port': 5432
})


def build_connection_str():
    while True:
        try:
            dbname = str(input("Enter dbname:"))
            user = str(input("Enter username:"))
            password = str(input("Enter password:"))
            host = str(input("Enter host (default is 5432):"))
            port = int(input("Enter port number (default is 5432):"))
        except:
            raise

        return "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**_conn_dict)

class PostgresHandler(DbHandler):
    pass

if __name__=='__main__':
    build_connection_str()
