#!/bin/bash

# Read PostgreSQL password from secrets file
password=$(head -n 1 secrets/.psql.pass)

# Clear the target HDFS directory if it exists
echo "Clearing HDFS target directory..."
hdfs dfs -rm -r -skipTrash project/warehouse || true 

# Import data from PostgreSQL to HDFS using Sqoop
echo "Importing data from PostgreSQL to HDFS using Sqoop..."
sqoop import-all-tables \
  -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
  --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team26_projectdb \
  --username team26 \
  --password $password \
  --compression-codec=snappy \
  --compress \
  --as-parquetfile \
  --warehouse-dir=project/warehouse \
  --m 4 \
  -Dparquet.int64.timestamp.unit=MILLIS \
  -Dhive.parquet.timestamp.skip.conversion=true \

echo "Data ingestion completed successfully!"