#!/bin/bash

mkdir -p ./downloads

# From https://dati.lombardia.it/stories/s/auv9-c2sj
declare -a Urls=(
"https://www.dati.lombardia.it/download/puwt-3xxh/application%2Fzip"
"https://www.dati.lombardia.it/download/wabv-jucw/application%2Fzip"
"https://www.dati.lombardia.it/download/5jdj-7x8y/application%2Fzip"
"https://www.dati.lombardia.it/download/h3i4-wm93/application%2Fzip"
"https://www.dati.lombardia.it/download/wp2f-5nw6/application%2Fzip"
"https://www.dati.lombardia.it/download/5mut-i45n/application%2Fzip"
"https://www.dati.lombardia.it/download/wr4y-c9ti/application%2Fzip"
"https://www.dati.lombardia.it/download/hsdm-3yhd/application%2Fzip"
"https://www.dati.lombardia.it/download/69yc-isbh/application%2Fzip"
"https://www.dati.lombardia.it/download/bpin-c7k8/application%2Fzip"
"https://www.dati.lombardia.it/download/7v3n-37f3/application%2Fzip"
"https://www.dati.lombardia.it/download/fdv6-2rbs/application%2Fzip"
"https://www.dati.lombardia.it/download/4t9j-fd8z/application%2Fzip"
"https://www.dati.lombardia.it/download/j2mz-aium/application%2Fzip"
)

echo "Downloading sensors readings data..."

for url in ${Urls[@]}; do
   wget -nc -P ./downloads --content-disposition $url
done

echo "Download complete"

echo "Unzipping"

unzip './downloads/*.zip' -d ./data/readings

echo "Unzipping complete"

echo "Downloading sensors description data..."

wget -nc -P ./data/sensors/ --content-disposition "https://www.dati.lombardia.it/api/views/ib47-atvt/rows.csv?accessType=DOWNLOAD&bom=true&format=true&delimiter=%3B"

echo "Download complete"
