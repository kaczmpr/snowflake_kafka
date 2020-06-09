# Snowflake and Kafka

This project download data from https://www.dati.lombardia.it/ about air pollution in Lombardia region.
Script `setup.sh` download data and then python kafka producer `producer.py` reads line, converts to json and send
data to python kafka consumer `consumer.py` and insert data via snowpipe to Snowflake table as a json.
This project is a little PoC of usage IoT with Python, Snowflake and Kafka



# Prerequisites
  - Docker [Install guide](https://docs.docker.com/get-docker/)
  - [Snowflake data warehouse](https://www.snowflake.com/)

#. Setup and configuration
  - Download and unzip data with `./setup.sh`
  - Create private key:
  `openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8`
  - Create public key:
  `openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub`
  - In Snowflake, modify user (with accountadmin role):
  `ALTER USER kafka_connector SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';`
  - Modify the following keys in `/kafka-connect/config.js`:
      - "snowflake.url.name"
      - "snowflake.user.name"
      - "snowflake.private.key"
  - Build and run entire stack: `docker-compose up --build`
  - Shut down services gracefuly: `docker-compose rm -svf`
