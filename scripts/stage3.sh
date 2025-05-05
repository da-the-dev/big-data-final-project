#!/bin/bash

source .venv/bin/activate

echo "Creating train/test sets..."

hdfs dfs -rm -r -skipTrash project/data/train || true 
hdfs dfs -rm -r -skipTrash project/data/test || true 
rm -f data/train.json
rm -f data/test.json

python scripts/prepare_data.py

hdfs dfs -cat project/data/train/*.json > data/train.json
hdfs dfs -cat project/data/test/*.json > data/test.json

echo "Data storage completed successfully!"