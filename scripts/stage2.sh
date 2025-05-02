#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

echo "Creating Hive database and tables..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team26 -p $password -f sql/db.hql > output/hive_results.txt 2> output/hive_errors.txt

echo "Running EDA queries..."
for i in {1..4}
do
   echo "Running query $i..."
   beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team26 -p $password -f sql/q$i.hql
done

echo "Preparing data for visualization..."
bash scripts/prepare_visualization.sh

echo "Stage 2 completed successfully!"