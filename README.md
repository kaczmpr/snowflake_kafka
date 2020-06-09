1. Prerequisites
  - Docker [Install guide](https://docs.docker.com/get-docker/)
  - [Snowflake data warehouse](https://www.snowflake.com/)
2. Setup and configuration
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
3. Build and run entire stack: `docker-compose up --build`
4. Shut down services gracefuly: `docker-compose rm -svf`
