#!/bin/bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

# echo "Cleaning up previous data..."
# hdfs dfs -rm -r -skipTrash project/data/train || true
# hdfs dfs -rm -r -skipTrash project/data/test || true
# rm -rf data/train.json data/test.json

# echo "Running data preparation pipeline..."
# spark-submit --master yarn scripts/prepare_data.py

# echo "Exporting data..."
# hdfs dfs -cat project/data/train/*.json > data/train.json
# hdfs dfs -cat project/data/test/*.json > data/test.json


# hdfs dfs -rm -r -skipTrash project/models/model1 || true
# hdfs dfs -rm -r -skipTrash project/models/model2 || true
# hdfs dfs -rm -r -skipTrash project/output/model1_predictions || true
# hdfs dfs -rm -r -skipTrash project/output/model2_predictions || true
# hdfs dfs -rm -r -skipTrash project/output/evaluation || true

# Clean local directories
# rm -rf models/model1 models/model2
rm -rf output/model1_predictions.csv output/model2_predictions.csv output/evaluation.csv

# echo "Training ML models..."
# spark-submit --master yarn scripts/train_models.py

echo "Exporting results..."

# hdfs dfs -get project/models/model1 models/
# hdfs dfs -get project/models/model2 models/

# hdfs dfs -getmerge project/output/model1_predictions/*.csv output/model1_predictions.csv
# hdfs dfs -getmerge project/output/model2_predictions/*.csv output/model2_predictions.csv
# hdfs dfs -getmerge project/output/evaluation/*.csv output/evaluation.csv

mkdir -p tmp/model1_predictions tmp/model2_predictions tmp/evaluation

hdfs dfs -get project/output/model1_predictions/*.csv tmp/model1_predictions/
hdfs dfs -get project/output/model2_predictions/*.csv tmp/model2_predictions/
hdfs dfs -get project/output/evaluation/*.csv tmp/evaluation/

echo "delay_from_typical_traffic,prediction" > output/model1_predictions.csv
awk 'FNR > 1 || NR == 1' tmp/model1_predictions/*.csv >> output/model1_predictions.csv

echo "delay_from_typical_traffic,prediction" > output/model2_predictions.csv
awk 'FNR > 1 || NR == 1' tmp/model2_predictions/*.csv >> output/model2_predictions.csv

echo "Model,RMSE,R2,MAE" > output/evaluation.csv
awk 'FNR > 1 || NR == 1' tmp/evaluation/*.csv >> output/evaluation.csv

rm -rf tmp


echo "Pipeline execution completed successfully."
