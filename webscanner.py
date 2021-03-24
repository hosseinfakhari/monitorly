import time
import os
import logging
from datetime import datetime
import requests
import yaml
from kafka import KafkaProducer
from json import dumps

website = 'https://aiven.io'


def validate_regex(content: str, regexp: str):
    if regexp is None:
        return None
    return True


def fetch(url: str, regexp: str) -> dict:
    now = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    try:
        response = requests.get(url)
        is_valid = validate_regex(response.text, regexp)
        return {'timestamp': now, 'url': url, 'status_code': response.status_code,
                'response_time': response.elapsed.total_seconds(), 'regex_valid': is_valid}
    except requests.exceptions.Timeout:
        logging.error(f'Timeout for ({url})')
    except requests.exceptions.TooManyRedirects:
        logging.warning(f'Too Many Redirect ({url})')
    except requests.exceptions.RequestException as e:
        logging.error(e)
    # TODO: bad practice
    return {}


if __name__ == '__main__':
    # TODO: validate yaml file
    # TODO: validate monitorly configs
    with open(r'monitorly.yaml') as config_file:
        config = yaml.full_load(config_file)

    sites = config.get('websites')
    site_url = sites.get('github').get('url')
    period = sites.get('github').get('period')
    regexp = sites.get('github').get('regexp')

    kafka_url = os.environ.get('KAFKA_URL', 'localhost:9092')
    kafka_topic = os.environ.get('KAFKA_TOPIC', 'weblog')
    producer = KafkaProducer(bootstrap_servers=kafka_url, value_serializer=lambda x: dumps(x).encode('utf-8'))

    while True:
        result = fetch(site_url, regexp)
        print(result)
        producer.send(kafka_topic, key=b'newlog', value=result)
        # TODO: run every {period} second! not wait after each response
        time.sleep(period)
