import csv
import logging
import confluent_kafka
import sys
from dataclasses import asdict
from os import path, listdir, environ
from time import sleep
from json import dumps, loads

from SensorRecord import SensorRecord

BOOTSTRAP_SERVERS = environ['BOOTSTRAP_SERVERS']

SENSORS_READINGS_TOPIC = environ['SENSORS_READINGS_TOPIC']
SENSORS_READINGS_PATH = environ['SENSORS_READINGS_PATH']
SENSORS_READINGS_FILES = environ.get('SENSORS_READINGS_FILES')

SENSORS_DESCRIPTION_TOPIC = environ['SENSORS_DESCRIPTION_TOPIC']
SENSORS_DESCRIPTION_PATH = environ.get('SENSORS_DESCRIPTION_PATH')

MESSAGE_DELAY = environ['MESSAGE_DELAY']

logging.basicConfig(level=logging.INFO)


def error_cb(kafka_error):
    print("ERR producer", kafka_error.name(), kafka_error.str(), file=sys.stderr)


def delivery_cb(err, msg):
    if err:
        print('message failed:', err, file=sys.stderr)


if __name__=='__main__':
    logging.info("Python producer started")
    while True:
        try:
            producer = confluent_kafka.Producer(
                {'bootstrap.servers': BOOTSTRAP_SERVERS,
                 'error_cb': error_cb,
                 'on_delivery': delivery_cb,
                 'group.id': 'mygroup',
                 'session.timeout.ms': 6000,
                 'queue.buffering.max.messages': 10000000,
                 'message.max.bytes': 2000000,
                 'batch.num.messages': 500,
                 'queue.buffering.max.ms': 500,
                 'auto.offset.reset': 'earliest'})
            break
        except (confluent_kafka.KafkaError._TRANSPORT, confluent_kafka.KafkaError._ALL_BROKERS_DOWN) as e:
            logging.warning('Waiting for broker...')
            sleep(5)

    if SENSORS_DESCRIPTION_PATH:
        sensors_description_files = [file for file in listdir(SENSORS_DESCRIPTION_PATH) if path.isfile(path.join(SENSORS_DESCRIPTION_PATH, file)) and file.endswith('.csv')]
        for file in sensors_description_files:
            with open(path.join(SENSORS_DESCRIPTION_PATH, file), 'r', encoding='utf-8-sig') as f:
                logging.info(f'Processing sensors description file: {f.name}')
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    msg=dumps(row)
                    try:
                        producer.produce(SENSORS_DESCRIPTION_TOPIC, value=msg)
                        producer.poll(0)
                    except (confluent_kafka.KafkaError._TRANSPORT, confluent_kafka.KafkaError._ALL_BROKERS_DOWN) as e:
                        logging.warning('Waiting for send messege to broker')
                    sleep(int(MESSAGE_DELAY))
                producer.flush()
            logging.info(f'Finished processing file: {f.name}')

    sensors_readings_files = [file for file in listdir(SENSORS_READINGS_PATH) if path.isfile(path.join(SENSORS_READINGS_PATH,file))]
    if SENSORS_READINGS_FILES:
        sensors_readings_files = SENSORS_READINGS_FILES.split(';')

    for file in filter(lambda filename : filename.endswith('.csv'), sensors_readings_files):
        try:
            with open(path.join(SENSORS_READINGS_PATH, file), 'r') as f:
                logging.info(f'Processing file: {f.name}')
                reader = csv.DictReader(f)
                for row in reader:
                    sensor_record = SensorRecord(**row)
                    msg=dumps(asdict(sensor_record))
                    try:
                        producer.produce(SENSORS_READINGS_TOPIC, value=msg)
                        producer.poll(0)
                    except (confluent_kafka.KafkaError._TRANSPORT, confluent_kafka.KafkaError._ALL_BROKERS_DOWN) as e:
                        logging.warning('Waiting for send messege to broker')
                    sleep(int(MESSAGE_DELAY))
                producer.flush()
                logging.info(f'Finished processing file: {f.name}')
        except FileNotFoundError as e:
            logging.error(f'Could not find file: {f.name}')
    logging.info(f'Finished processing all files')
