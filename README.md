# Monitorly
### monitorly checks website availability periodically and produce log info into kafka topic, then a consumer consume that logs and persists into Postgres Database.


## Running Service:

requirements:
* Kafka
* Postgresql

environment variables:
* KAFKA_URI
* KAFKA_TOPIC
* KAFKA_CA 
* KAFKA_SERVICE_CERT
* KAFKA_SERVICE_KEY

#### Running Options:

docker compose: 
```bash
docker-compose up -d
```

unix machine:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

python webscanner.py &
python consumer.py &
```