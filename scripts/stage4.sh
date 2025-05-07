#!/bin/bash
password=$(head -n 1 secrets/.hive.pass)

echo "Creating ML tables..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
   -n team26 -p "$password" \
   --hivevar DB_NAME=team26_projectdb \
   --hivevar MODEL1_LOCATION='/user/team26/project/output/model1_predictions' \
   --hivevar MODEL2_LOCATION='/user/team26/project/output/model2_predictions' \
   --hivevar EVAL_LOCATION='/user/team26/project/output/evaluation' \
   -f sql/create_ml_tables.hql \
   > "output/ml_table_creation.log" 2> "output/ml_table_creation.err"
