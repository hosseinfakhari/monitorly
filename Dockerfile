FROM python:3.9-buster

RUN apt update;apt install -y libpq-dev;
RUN mkdir /app

WORKDIR /app
COPY . .

RUN pip install -U pip; pip install -r requirements.txt
