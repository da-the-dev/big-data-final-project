USE team26_projectdb;

DROP TABLE IF EXISTS q14_results;

CREATE EXTERNAL TABLE q14_results(
    metric_a   STRING,
    metric_b   STRING,
    corr_value DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q14';

INSERT INTO q14_results
SELECT 'delay_from_typical_traffic', 'distance',
       corr(delay_from_typical_traffic, distance)                 AS corr_value
FROM traffic_partitioned
UNION ALL
SELECT 'delay_from_typical_traffic', 'precipitation',
       corr(delay_from_typical_traffic, precipitation)
FROM traffic_partitioned
UNION ALL
SELECT 'distance', 'precipitation',
       corr(distance, precipitation)
FROM traffic_partitioned
UNION ALL
SELECT 'temperature', 'humidity',
       corr(temperature, humidity)
FROM traffic_partitioned;

INSERT OVERWRITE DIRECTORY 'project/output/q14'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q14_results;
