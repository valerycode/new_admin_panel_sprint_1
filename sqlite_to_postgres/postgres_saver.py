import logging
from dataclasses import asdict, astuple

from config import TABLES
from psycopg2.extensions import connection as _connection
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

DATA_SAVED = 'Данные таблицы {table} успешно записаны в Postgres.'


class PostgresSaver:

    def __init__(self, pg_conn: _connection) -> None:
        self.connection = pg_conn
        self.cursor = pg_conn.cursor()

    def save_all_data(self, data: dict) -> None:
        """Загружает данные в Postgres"""
        for table in data:
            dataclasses_fields = asdict(TABLES[table]()).keys()
            fields = ', '.join(dataclasses_fields)
            rows = data[table]
            insert_query = ("INSERT INTO content.{table} ({fields})"
                            " VALUES %s ON CONFLICT (id) DO NOTHING;").format(
                            table=table, fields=fields)
            execute_values(
                self.cursor,
                insert_query,
                [astuple(row) for row in rows]
            )
            self.connection.commit()
            logger.debug(DATA_SAVED.format(table=table))
