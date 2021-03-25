"""Webscanner checks website availability and produce its data into kafka topic"""
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


def graceful_shutdown(signal, frame):
    logger.warning(f'Termination Signal({signal}) Captured: {frame}')
    logger.warning('Graceful Shutting down....')
    for thread in threads:
        thread.join()
    logger.info('Exiting...')
    sys.exit(0)


def fetch(producer, kafka_topic, url: str, regexp: str) -> dict:
    now = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    try:
        response = requests.get(url)
        is_valid = validate_regex(response.text, regexp)
        result = {'timestamp': now, 'url': url, 'status_code': response.status_code,
                  'response_time': response.elapsed.total_seconds(), 'regex_valid': is_valid}
        logger.info(result)
        lock.acquire()
        producer.send(kafka_topic, result)
        lock.release()
    except requests.exceptions.Timeout:
        logging.error(f'Timeout for ({url})')
    except requests.exceptions.TooManyRedirects:
        logging.warning(f'Too Many Redirect ({url})')
    except requests.exceptions.RequestException as e:
        logging.error(e)


def monitor_website(producer, kafka_topic, url, regexp):
    t = threading.Thread(target=fetch, args=(producer, kafka_topic, url, regexp))
    t.start()
    threads.append(t)


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
