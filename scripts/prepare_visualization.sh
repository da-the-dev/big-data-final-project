#!/bin/bash

# Set the number of queries
NUM_QUERIES=4

# Clean up existing output files if they exist
echo "Cleaning up previous results..."
for i in $(seq 1 $NUM_QUERIES); do
    rm -f output/q$i.csv
done

# Create output directory if it doesn't exist
mkdir -p output

# Create headers for each query result
echo "severity,avg_delay,count" > output/q1.csv
echo "weather_event,avg_delay,count" > output/q2.csv
echo "hour_of_day,avg_delay,count" > output/q3.csv
echo "state,city,avg_delay,count" > output/q4.csv

# Concatenate HDFS output with headers
for i in $(seq 1 $NUM_QUERIES); do
    hdfs dfs -cat project/output/q$i/* >> output/q$i.csv
done

echo "CSV files prepared for visualization"
