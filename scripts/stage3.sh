#!/bin/bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

echo "Creating train/test sets..."

hdfs dfs -rm -r -skipTrash project/data/train || true 
hdfs dfs -rm -r -skipTrash project/data/test || true 
rm -f data/train.json
rm -f data/test.json

spark-submit --master yarn scripts/prepare_data.py

hdfs dfs -getmerge project/data/train data/train.json
hdfs dfs -getmerge project/data/test data/test.json
