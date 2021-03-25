import os
import psycopg2

from utils.logutil import get_logger

logger = get_logger(__name__)


def get_database_dsn():
    if not os.environ.get('POSTGRES_URI'):
        raise RuntimeError('POSTGRES_URI environment variable is not set')
    return os.environ.get('POSTGRES_URI')


def get_database_connection():
    try:
        connection = psycopg2.connect(get_database_dsn())
        return connection
    except (Exception, psycopg2.Error) as error:
        raise RuntimeError(f'Database Connection Error: {error}')


def initialize_database(db_connection):
    try:
        cursor = db_connection.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS weblog(
                                    url VARCHAR (255) NOT NULL,
                                    checked_at timestamp NOT NULL,
                                    status_code integer NOT NULL,
                                    response_time float NOT NULL,
                                    valid_regex BOOLEAN NULL,
                                    UNIQUE (url, checked_at));
                                    """)
        cursor.close()
        db_connection.commit()
    except (Exception, psycopg2.Error) as error:
        raise RuntimeError(f'Database Initialization Error: {error}')


def save(connection, url, timestamp, status_code, response_time, regex_valid):
    cursor = connection.cursor()
    try:
        query = f'INSERT INTO weblog (url, checked_at, status_code, response_time, valid_regex) ' \
                f'VALUES (\'{url}\', \'{timestamp}\', {status_code}, {response_time}, {regex_valid})'
        cursor.execute(query)
        connection.commit()
        logger.info(f'object saved into database')
    except (Exception, psycopg2.Error) as error:
        connection.rollback()
        logger.error(f'Consumer Operation Error: {error}')
    finally:
        cursor.close()
