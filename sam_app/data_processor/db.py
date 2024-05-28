from sqlalchemy import create_engine

from config import MYSQL_URL

mysql_engine = create_engine(MYSQL_URL, echo=False)
