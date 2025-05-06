USE team26_projectdb;

DROP TABLE IF EXISTS ${hivevar:RESULT_TABLE};

CREATE EXTERNAL TABLE ${hivevar:RESULT_TABLE} (
    metric_a STRING,
    metric_b STRING,
    corr_value DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hivevar:WAREHOUSE_PATH}';

INSERT INTO ${hivevar:RESULT_TABLE}
SELECT 'delay_from_typical_traffic', 'distance',
       corr(delay_from_typical_traffic, distance) AS corr_value
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

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_PATH}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM ${hivevar:RESULT_TABLE};
