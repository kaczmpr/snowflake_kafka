FROM confluentinc/cp-kafka-connect:5.5.0

COPY . .

RUN mkdir -p /usr/share/java/snowflake-kafka-connector
RUN confluent-hub install --no-prompt snowflakeinc/snowflake-kafka-connector:1.2.3 --component-dir /usr/share/java/snowflake-kafka-connector
