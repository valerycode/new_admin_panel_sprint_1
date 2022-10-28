import logging
import sqlite3

from config import TABLES

logger = logging.getLogger(__name__)

DATA_IS_FETCHED = 'Данные таблицы {table} успешно выгружены из SQLite.'


class SQLiteExtractor:

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.cursor = connection.cursor()
        self.size = 1000

    def extract_movies(self) -> dict:
        """Метод выгрузки данных из SQLite"""
        data = {}
        for table in TABLES:
            table_data = []
            table_class = TABLES.get(table)
            if not table_class:
                continue
            self.cursor.execute("SELECT * FROM {table};".format(table=table))
            while rows := self.cursor.fetchmany(self.size):
                table_data += [table_class(*row) for row in rows]
            data[table] = table_data
            logger.debug(DATA_IS_FETCHED.format(table=table))
        return data
