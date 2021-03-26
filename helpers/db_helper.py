"""
Helper method for creating database connection, initialization and store
"""
import os
import psycopg2

from utils.logutil import get_logger

logger = get_logger(__name__)


def get_database_dsn():
    """
    Read Postgres DSN from Env and return it.
    """
    if not os.environ.get('POSTGRES_URI'):
        raise RuntimeError('POSTGRES_URI environment variable is not set')
    return os.environ.get('POSTGRES_URI')


def get_database_connection():
    """
    Establishing Connection to Database and returns the connection
    """
    try:
        connection = psycopg2.connect(get_database_dsn())
        return connection
    except (Exception, psycopg2.Error) as error:
        raise RuntimeError(
            f'Database Connection Error: {error}') from psycopg2.Error


def initialize_database(db_connection):
    """
    Creating Table for monitorly data structure.
    """
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
        raise RuntimeError(f'Database Initialization Error:{error}') from error


def save(connection, site_metric: dict):
    """
    Storing website metrics in postgres
    """
    cursor = connection.cursor()
    try:
        url = site_metric.get('url')
        timestamp = site_metric.get('timestamp')
        status_code = site_metric.get('status_code')
        response_time = site_metric.get('response_time')
        regex_valid = site_metric.get('regex_valid')

        query = f'INSERT INTO weblog (url, checked_at, ' \
                f'status_code, response_time, valid_regex) ' \
                f'VALUES (\'{url}\', \'{timestamp}\', {status_code}, ' \
                f'{response_time}, {regex_valid})'

        cursor.execute(query)
        connection.commit()
        logger.info('object saved into database')
    except (Exception, psycopg2.Error) as error:
        connection.rollback()
        raise RuntimeError(f'Consumer Operation Error: {error}') from error
    finally:
        cursor.close()
