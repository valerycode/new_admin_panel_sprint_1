import logging
import sqlite3
from contextlib import closing

import psycopg2
from config import DB_PATH, conn_context, dsl
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from sqlite_to_postgres.postgres_saver import PostgresSaver
from sqlite_to_postgres.sqlite_extractor import SQLiteExtractor

logging.basicConfig(
    level=logging.DEBUG,
    filename='load_data_logs.log',
    format='%(asctime)s, %(levelname)s, %(message)s, %(name)s',
    filemode='w'
)

logger = logging.getLogger(__name__)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)
    data = sqlite_extractor.extract_movies()
    postgres_saver.save_all_data(data)


if __name__ == '__main__':
    try:
        with conn_context(DB_PATH) as sqlite_conn, closing(
                psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
    except sqlite3.Error as error:
        logger.error(f'Возникла ошибка при подключении к SQLite: {error}')
    except psycopg2.DatabaseError as error:
        logger.error(f'Возникла ошибка при подключении к PostgreSQL: {error}')
    except Exception as error:
        logger.error(
            f'При выполнении работы функции'
            f' load_from_sqlite возникла ошибка: {error}'
        )
