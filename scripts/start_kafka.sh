#!/bin/bash

# Load .env
set -a
source "$(dirname "$0")/../.env"
set +a

echo "KAFKA_DIR: $KAFKA_DIR" 

# Check if Kafka is already running
if lsof -Pi :9092 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "Kafka is already running on port 9092"
    exit 0
fi

echo "Starting Kafka..."
$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties


#bin/kafka-server-start.sh config/server.properties


