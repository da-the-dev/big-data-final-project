#!/bin/bash

# Read PostgreSQL password from secrets file
password=$(head -n 1 secrets/.psql.pass)

# Clear the target HDFS directory if it exists
echo "Clearing HDFS target directory..."
hdfs dfs -rm -r project/warehouse || true 

# Import data from PostgreSQL to HDFS using Sqoop
echo "Importing data from PostgreSQL to HDFS using Sqoop..."
sqoop import-all-tables \
  --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team26_projectdb \
  --username team26 \
  --password $password \
  --compression-codec=snappy \
  --compress \
  --as-parquetfile \ # For analytical queries
  --warehouse-dir=project/warehouse \
  --m 4

# only for avro
# mv -f *.avsc output/
# mv -f *.java output/

# hdfs dfs -mkdir -p project/warehouse/avsc
# hdfs dfs -put output/*.avsc project/warehouse/avsc

echo "Data ingestion completed successfully!"