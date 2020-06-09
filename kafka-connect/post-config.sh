#!/bin/bash

while : ; do
  curl_status=$(curl -s -o /dev/null -w %{http_code} localhost:8083/connectors)
  echo -e $(date) " Kafka Connect listener HTTP state: " $curl_status " (waiting for 200)"
  if [ $curl_status -eq 200 ] ; then
    break
  fi
  sleep 5
done
curl -X POST -H "Content-Type: application/json" localhost:8083/connectors -d @config.json
sleep infinity
