"""
Webscanner checks website availability and produce its data into kafka topic
"""
import time
import threading
import signal
import sys
import os
import logging
from datetime import datetime

import requests
import yaml

from helpers import kafka_helpers
from utils.logutil import get_logger
from utils.validator import validate_regex

threads = []
lock = threading.Lock()
logger = get_logger(__name__)


def graceful_shutdown(input_signal, frame):
    """
    Handling SIGINT and SIGTERM from OS and populate all thread before
    exiting.
    """
    logger.warning('Termination Signal(%s) Captured: %s', input_signal, frame)
    logger.warning('Graceful Shutting down....')
    for thread in threads:
        thread.join()
    logger.info('Exiting...')
    sys.exit(0)


def fetch(kafka_producer, topic, url: str, regex: str) -> dict:
    """
    Fetching web url and checking for regex pattern if provided.
    """
    now = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    try:
        response = requests.get(url)
        is_valid = validate_regex(response.text, regex)
        result = {
            'timestamp': now,
            'url': url,
            'status_code': response.status_code,
            'response_time': response.elapsed.total_seconds(),
            'regex_valid': is_valid
        }
        logger.info(result)
        lock.acquire()
        kafka_producer.send(topic, result)
        lock.release()
    except requests.exceptions.Timeout:
        logging.error('Timeout for (%s)', url)
    except requests.exceptions.TooManyRedirects:
        logging.warning('Too Many Redirect (%s)', url)
    except requests.exceptions.RequestException as error:
        logging.error(error)


def monitor_website(kafka_producer, topic, url, regex):
    """
    Monitors website availability within a thread.
    """
    thread = threading.Thread(
        target=fetch,
        args=(kafka_producer, topic, url, regex)
    )
    thread.start()
    threads.append(thread)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    # TODO: validate yaml file
    # TODO: validate monitorly configs
    with open(r'monitorly.yaml') as config_file:
        config = yaml.full_load(config_file)

    target = config.get('target')
    site_url = target.get('url')
    period = target.get('period')
    regexp = target.get('regexp')

    if not os.environ.get('KAFKA_TOPIC'):
        raise RuntimeError('KAFKA_TOPIC environment variable is not set')
    kafka_topic = os.environ.get('KAFKA_TOPIC', 'weblog')
    producer = kafka_helpers.get_producer()

    while True:
        monitor_website(producer, kafka_topic, site_url, regexp)
        time.sleep(period)
