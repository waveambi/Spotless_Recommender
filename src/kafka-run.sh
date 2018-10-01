#!/usr/bin/env bash

S3CONFIGFILE=$PWD/config/s3bucket.config

TOPIC="topic_test"
NUM_PARTITIONS=3
REPL_FACTOR=2
RETENTION=3600000
ZOOKEEPER_IP="localhost:2181"
BROKERS_IP="localhost:9092"

case $1 in
  --create)
    kafka-topics.sh --create --zookeeper $ZOOKEEPER_IP \
                             --topic $TOPIC \
                             --partitions $NUM_PARTITIONS \
                             --replication-factor $REPL_FACTOR \
                             --config retention.ms=$RETENTION
    ;;
  --produce)
    python kafka/main_produce.py $KAFKACONFIGFILE $S3CONFIGFILE &
    ;;
  --describe)
    kafka-topics.sh --describe --zookeeper $ZOOKEEPER_IP --topic $TOPIC
    ;;
  --delete)
    kafka-topics.sh --delete --zookeeper $ZOOKEEPER_IP --topic $TOPIC
    ;;
  --console-consume)
    kafka-console-consumer.sh --bootstrap-server $BROKERS_IP --from-beginning --topic $TOPIC
    ;;
  *)
    echo "Usage: ./kafka-run.sh [--create|--delete|--describe|--produce|--console-consume]"
    ;;
esac