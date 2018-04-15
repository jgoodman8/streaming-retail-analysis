#!/bin/bash

FILE=./src/main/resources/production.csv
TOPIC=purchases
NODE=node1
PORT=9092

if [ "$#" -ge 1 ]; then
    FILE=$1
fi

if [ "$#" -ge 2 ]; then
    TOPIC=$2
fi

if [ "$#" -ge 3 ]; then
    NODE=$3
fi

if [ "$#" -ge 4 ]; then
    PORT=$4
fi

java -classpath target/scala-2.11/anomalyDetection-assembly-1.0.jar es.dmr.uimp.simulation.InvoiceDataProducer ${FILE} ${TOPIC} ${NODE}:${PORT}
