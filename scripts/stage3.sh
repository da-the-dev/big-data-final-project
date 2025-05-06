#!/bin/bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

echo "Cleaning up previous data..."
# Remove existing data and models
hdfs dfs -rm -r -skipTrash project/data/train || true
hdfs dfs -rm -r -skipTrash project/data/test || true
hdfs dfs -rm -r -skipTrash project/models/model1 || true
hdfs dfs -rm -r -skipTrash project/models/model2 || true
hdfs dfs -rm -r -skipTrash project/output/model1_predictions || true
hdfs dfs -rm -r -skipTrash project/output/model2_predictions || true
hdfs dfs -rm -r -skipTrash project/output/evaluation || true

# Clean local directories
rm -rf data/train.json data/test.json
rm -rf models/model1 models/model2
rm -rf output/model1_predictions.csv output/model2_predictions.csv output/evaluation.csv

echo "Running data preparation pipeline..."
spark-submit --master yarn scripts/prepare_data.py

echo "Training ML models..."
spark-submit --master yarn scripts/train_models.py

echo "Exporting results..."
hdfs dfs -cat project/data/train/*.json > data/train.json
hdfs dfs -cat project/data/test/*.json > data/test.json

hdfs dfs -get project/models/model1 models/
hdfs dfs -get project/models/model2 models/

hdfs dfs -getmerge project/output/model1_predictions output/model1_predictions.csv
hdfs dfs -getmerge project/output/model2_predictions output/model2_predictions.csv
hdfs dfs -getmerge project/output/evaluation output/evaluation.csv

echo "Pipeline execution completed successfully."
