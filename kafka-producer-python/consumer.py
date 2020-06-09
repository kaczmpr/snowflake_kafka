import logging
import confluent_kafka
import sys
from json import dumps, loads
from time import sleep
from os import environ

SERVER = environ['BOOTSTRAP_SERVERS']
SENSORS_READINGS_TOPIC = environ['SENSORS_READINGS_TOPIC']
SENSORS_DESCRIPTION_TOPIC = environ['SENSORS_DESCRIPTION_TOPIC']


def error_cb(kafka_error):
    print("ERR consumer", kafka_error.name(), kafka_error.str(), file=sys.stderr)


logging.basicConfig(level=logging.INFO)

if __name__=='__main__':
    logging.info('Python consumers started')
    while True:
        try:
            consumer_surveys = confluent_kafka.Consumer(
                {'bootstrap.servers': SERVER,
                 'error_cb': error_cb,
                 'group.id': 'mygroup',
                 'session.timeout.ms': 6000,
                 'queue.buffering.max.messages': 10000000,
                 'message.max.bytes': 2000000,
                 'batch.num.messages': 500,
                 'queue.buffering.max.ms': 500,
                 'auto.offset.reset': 'earliest'})

            consumer_surveys.subscribe([SENSORS_DESCRIPTION_TOPIC])
            logging.info(f'Python consumer subscribed to {SENSORS_DESCRIPTION_TOPIC}')

            consumer_readings = confluent_kafka.Consumer(
                {'bootstrap.servers': SERVER,
                 'error_cb': error_cb,
                 'group.id': 'mygroup',
                 'session.timeout.ms': 6000,
                 'queue.buffering.max.messages': 10000000,
                 'message.max.bytes': 2000000,
                 'batch.num.messages': 500,
                 'queue.buffering.max.ms': 500,
                 'auto.offset.reset': 'earliest'})

            consumer_readings.subscribe([SENSORS_READINGS_TOPIC])
            logging.info(f'Python consumer subscribed to {SENSORS_READINGS_TOPIC}')
            while True:
                message_surveys = consumer_surveys.poll(1)
                message_readings = consumer_readings.poll(1)
        except confluent_kafka.KafkaException as e:
            logging.warning(e)
            sleep(5)
