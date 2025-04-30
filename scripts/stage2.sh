#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team26 -p $password -f sql/db.hql > output/hive_results.txt 2> output/hive_errors.txt
