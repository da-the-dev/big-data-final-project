#!/bin/bash

# Set the number of queries
NUM_QUERIES=15

# Clean up existing output files if they exist
echo "Cleaning up previous results..."
for i in $(seq 1 $NUM_QUERIES); do
    rm -f output/q$i.csv
done

# Create output directory if it doesn't exist
mkdir -p output

# Create headers for each query result
echo "severity,avg_delay,count" > output/q1.csv
echo "weather_conditions,avg_delay,count" > output/q2.csv
echo "hour_of_day,avg_delay,count" > output/q3.csv
echo "state,city,avg_delay,count" > output/q4.csv
echo "day_of_week,avg_delay,count" > output/q5.csv
echo "year,month,avg_delay,count" > output/q6.csv
echo "state,avg_delay,count" > output/q7.csv
echo "visibility_bucket,avg_delay,count" > output/q8.csv
echo "distance_bucket,avg_delay,count" > output/q9.csv
echo "severity,avg_duration,avg_delay,count" > output/q10.csv
echo "weather_event,severe_count,total_count,share_severe" > output/q11.csv
echo "column_name,nulls,total,pct_nulls" > output/q12.csv
echo "lat_bucket,lng_bucket,avg_delay,count" > output/q13.csv
echo "metric_a,metric_b,corr_value" > output/q14.csv
echo "year,state,avg_delay" > output/q15.csv

# Concatenate HDFS output with headers
for i in $(seq 1 $NUM_QUERIES); do
    hdfs dfs -cat project/output/q$i/* >> output/q$i.csv
done

echo "CSV files prepared for visualization"
