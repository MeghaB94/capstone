from sqlalchemy import create_engine, inspect
from config import MYSQL_URL

mysql_engine = create_engine(MYSQL_URL, echo=False)


def check_table_exists(table_name):
    return inspect(mysql_engine).has_table(table_name)
