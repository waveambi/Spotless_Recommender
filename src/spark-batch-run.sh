#!/bin/bash
S3CONFIGFILE=$PWD/config/s3bucket.config
PSQLCONFIGFILE=$PWD/config/postgresql.config
AUX_FILES=$PWD/helpers/helper.py

spark-submit --master spark://ip-10-0-0-7:7077 \
             --jars $PWD/postgresql-42.2.5.jar \
             --py-files $AUX_FILES \
             --driver-memory 4G \
             --executor-memory 4G \
             batch_processing/main_batch.py \
             $S3CONFIGFILE $PSQLCONFIGFILE
    