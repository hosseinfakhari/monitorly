"""
Helper methods for creating the kafka-python KafkaProducer and
KafkaConsumer objects.
Adapted from Heroku Kafka Helper
"""

import os
from json import dumps, loads
from kafka import KafkaProducer, KafkaConsumer


def get_broker() -> str:
    """
    Read kafka URL from Env and return it. e.g localhost:9092
    """
    if not os.environ.get('KAFKA_URI'):
        raise RuntimeError('KAFKA_URI environment variable is not set')
    return os.environ.get('KAFKA_URI')


def get_kafka_ssl_context() -> dict:
    """
    Expects the following variables to be set KAFKA_CA,
    KAFKA_SERVICE_CERT, KAFKA_SERVICE_KEY
    to file locations of each of the corresponding files
    KAFKA_CA - ca.pem
    KAFKA_SERVICE_CERT - service.cert
    KAFKA_SERVICE_KEY - service.key
    """
    if not os.environ.get('KAFKA_CA'):
        raise RuntimeError('The KAFKA_CA config variable is not set.')
    if not os.environ.get('KAFKA_SERVICE_CERT'):
        raise RuntimeError(
            'The KAFKA_SERVICE_CERT config variable is not set.')
    if not os.environ.get('KAFKA_SERVICE_KEY'):
        raise RuntimeError('The KAFKA_SERVICE_KEY config variable is not set.')

    ssl_info = {"ssl_cafile": os.environ["KAFKA_CA"],
                "ssl_certfile": os.environ["KAFKA_SERVICE_CERT"],
                "ssl_keyfile": os.environ["KAFKA_SERVICE_KEY"]}

    return ssl_info


def get_producer() -> KafkaProducer:
    """
    Return a KafkaProducer that uses the SSLContext created with
    create_ssl_context.
    """
    ssl_info = get_kafka_ssl_context()
    producer = KafkaProducer(
        bootstrap_servers=get_broker(),
        security_protocol="SSL",
        ssl_cafile=ssl_info["ssl_cafile"],
        ssl_certfile=ssl_info["ssl_certfile"],
        ssl_keyfile=ssl_info["ssl_keyfile"],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    return producer


def get_consumer(topic=None,
                 client_id='monitorly-consumer',
                 group_id='monitorly',
                 auto_offset_reset='earliest') -> KafkaConsumer:
    """
    Return a KafkaConsumer that uses the SSLContext created with
    create_ssl_context.
    """
    ssl_info = get_kafka_ssl_context()
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=get_broker(),
                             client_id=client_id,
                             group_id=group_id,
                             auto_offset_reset=auto_offset_reset,
                             security_protocol="SSL",
                             ssl_cafile=ssl_info["ssl_cafile"],
                             ssl_certfile=ssl_info["ssl_certfile"],
                             ssl_keyfile=ssl_info["ssl_keyfile"],
                             value_deserializer=lambda x: loads(
                                 x.decode('utf-8'))
                             )
    return consumer
