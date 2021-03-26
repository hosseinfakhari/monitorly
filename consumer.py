"""
Main application for consumer side of monitorly
"""
import os

from utils.logutil import get_logger
from helpers.kafka_helpers import get_consumer
from helpers.db_helper import (
    get_database_connection,
    initialize_database,
    save
)


logger = get_logger(__name__)

if __name__ == '__main__':
    # Establishing database connection
    db_connection = get_database_connection()
    initialize_database(db_connection)
    if not os.environ.get('KAFKA_TOPIC'):
        raise RuntimeError('KAFKA_TOPIC environment variable is not set')
    kafka_topic = os.environ.get('KAFKA_TOPIC', 'weblog')

    consumer = get_consumer(kafka_topic)
    while True:
        try:
            for msg in consumer:
                timestamp = msg.value.get('timestamp')
                url = msg.value.get('url')
                status_code = msg.value.get('status_code')
                response_time = msg.value.get('response_time')
                REGEX_VALID = msg.value.get('regex_valid')

                if timestamp is None or url is None or status_code is None \
                        or response_time is None:
                    logger.error('invalid data format consumed')
                    continue

                # Handling python None type for postgres
                if REGEX_VALID is None:
                    REGEX_VALID = 'null'
                elif REGEX_VALID:
                    REGEX_VALID = 'true'
                else:
                    REGEX_VALID = 'false'

                site_metric = {
                    'url': url,
                    'timestamp': timestamp,
                    'status_code': status_code,
                    'response_time': response_time,
                    'regex_valid': REGEX_VALID
                }

                save(db_connection, site_metric)
                consumer.commit()
                logger.info('consumer committed')
        except Exception as error:
            logger.error('Consumer Operation Error: %s', error)
            continue
