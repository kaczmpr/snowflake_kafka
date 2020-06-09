FROM python:3

WORKDIR /usr/src/app/kafka-producer-python-confluent

COPY ./requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .
