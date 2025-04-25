#!/bin/bash

# Inserting everything to the database
python scripts/build_projectdb.py

password=$(head -n 1 secrets/.psql.pass)

# Importing everything to the database
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team26_projectdb --username team26 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1
