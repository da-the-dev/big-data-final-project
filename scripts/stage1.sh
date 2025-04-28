#!/bin/bash

# Inserting everything to the database
source .venv/bin/activate
python scripts/build_projectdb.py

# Delete previous data
hdfs dfs -rm project/warehouse

# Importing everything to the database
password=$(head -n 1 secrets/.psql.pass)
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team26_projectdb --username team26 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1 --exclude-columns state

mv -f *.avsc output/
mv -f *.java output/

hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put output/*.avsc project/warehouse/avsc
