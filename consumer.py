import os
from json import loads
import psycopg2
from psycopg2 import Error
from kafka import KafkaConsumer

if __name__ == '__main__':
    hostname = os.environ.get('POSTGRES_HOST', 'localhost')
    username = os.environ.get('POSTGRES_USER', 'postgres')
    password = os.environ.get('POSTGRES_PASS', 'postgres')
    database = os.environ.get('POSTGRES_DB', 'monitorly')
    try:
        db_connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
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

        kafka_url = os.environ.get('KAFKA_URL', 'localhost:9092')
        kafka_topic = os.environ.get('KAFKA_TOPIC', 'weblog')
        client_id = 'monitorly-consumer'
        auto_offset_reset = 'earliest'
        consumer = KafkaConsumer(kafka_topic,
                                 bootstrap_servers=kafka_url,
                                 client_id=client_id,
                                 auto_offset_reset=auto_offset_reset,
                                 value_deserializer=lambda x: loads(x.decode('utf-8'))
                                 )
        while True:
            try:
                for msg in consumer:
                    cursor = db_connection.cursor()

                    timestamp = msg.value.get('timestamp')
                    url = msg.value.get('url')
                    status_code = msg.value.get('status_code')
                    response_time = msg.value.get('response_time')
                    regex_valid = msg.value.get('regex_valid')
                    if regex_valid is None:
                        regex_valid = 'null'
                    elif regex_valid:
                        regex_valid = 'true'
                    else:
                        regex_valid = 'false'

                    query = f'INSERT INTO weblog (url, checked_at, status_code, response_time, valid_regex) ' \
                            f'VALUES (\'{url}\', \'{timestamp}\', {status_code}, {response_time}, {regex_valid})'
                    cursor.execute(query)

                    cursor.close()
                    db_connection.commit()
            except Exception as e:
                print(e)
                continue

    except (Exception, Error) as error:
        print(f'Error: {error}')
        raise error
