#!/usr/bin/env bash
S3CONFIGFILE=$PWD/config/s3bucket.config
PSQLCONFIGFILE=$PWD/config/postgresql.config
AUX_FILES=$PWD/helpers/helper.py

SCHEMAFILE=$PWD/config/schema_for_streaming.config
STREAMCONFIGFILE=$PWD/config/streaming.config
KAFKACONFIGFILE=$PWD/config/kafka.config

spark-submit --master spark://ip-10-0-0-7:7077 \
             --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
             --jars $PWD/postgresql-42.2.5.jar \
             --py-files $AUX_FILES \
             --driver-memory 4G \
             --executor-memory 4G \
             streaming/main_stream.py \
             $KAFKACONFIGFILE $STREAMCONFIGFILE $PSQLCONFIGFILE