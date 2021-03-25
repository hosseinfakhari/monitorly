import os
from json import dumps, loads
from kafka import KafkaProducer, KafkaConsumer


def get_broker() -> str:
    if not os.environ.get('KAFKA_URL'):
        raise RuntimeError('KAFKA_URL environment variable is not set')
    # TODO: check this!
    # kafka_host, kafka_port = os.environ.get('KAFKA_URL').split('__')
    # return f'{kafka_host}:{kafka_port}'
    return os.environ.get('KAFKA_URL')


def get_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=get_broker(),
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    return producer


def get_consumer(topic=None,
                 client_id='monitorly-consumer',
                 group_id='monitorly',
                 auto_offset_reset='earliest') -> KafkaConsumer:
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=get_broker(),
                             client_id=client_id,
                             group_id=group_id,
                             auto_offset_reset=auto_offset_reset,
                             value_deserializer=lambda x: loads(x.decode('utf-8'))
                             )
    return consumer
