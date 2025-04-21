#!/bin/bash

curl -L -o data/us-traffic-congestions-2016-2022.zip https://www.kaggle.com/api/v1/datasets/download/sobhanmoosavi/us-traffic-congestions-2016-2022

unzip data/us-traffic-congestions-2016-2022.zip -d data/
mv data/us-traffic-congestions-2016-2022/us_congestion_2016_2022.csv data/
rm -rf data/us-traffic-congestions-2016-2022
rm data/us-traffic-congestions-2016-2022.zip

# Inserting everything to the database  
python scripts/build_projectdb.py

password=$(head -n 1 secrets/.psql.pass)

# Importing everything to the database
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team26 --username teamx --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1
