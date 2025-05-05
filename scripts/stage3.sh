#!/bin/bash
export HADOOP_CONF_DIR=$HADOOP_HOME%/etc/hadoop

echo "Creating train/test sets..."

hdfs dfs -rm -r -skipTrash project/data/train || true 
hdfs dfs -rm -r -skipTrash project/data/test || true 
rm -f data/train.json
rm -f data/test.json

spark-submit --master yarn scripts/prepare_data.py

hdfs dfs -cat project/data/train/*.json > data/train.json
hdfs dfs -cat project/data/test/*.json > data/test.json
