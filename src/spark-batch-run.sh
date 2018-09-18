#!/bin/bash
S3CONFIGFILE=$PWD/config/s3bucket.config
PSQLCONFIGFILE=$PWD/config/postgresql.config
spark-submit --master spark://ip-10-0-0-7:7077 \
             --driver-memory 4G \
             --executor-memory 4G \
             batch_processing/main_batch.py \
             $S3CONFIGFILE $PSQLCONFIGFILE
    