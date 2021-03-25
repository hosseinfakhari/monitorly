import os
import logging
from helpers.kafka_helpers import get_consumer
from helpers.db_helper import (
    get_database_connection,
    initialize_database,
    save
)
from utils.logutil import get_logger

logger = get_logger(__name__)

if __name__ == '__main__':
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
                regex_valid = msg.value.get('regex_valid')

                if timestamp is None or url is None or status_code is None or response_time is None:
                    logger.error('invalid data format consumed')
                    continue

                if regex_valid is None:
                    regex_valid = 'null'
                elif regex_valid:
                    regex_valid = 'true'
                else:
                    regex_valid = 'false'

                save(db_connection, url, timestamp, status_code, response_time, regex_valid)
                consumer.commit()
                logger.info(f'consumer committed')
        except Exception as error:
            logger.error(f'Consumer Operation Error: {error}')
            continue
