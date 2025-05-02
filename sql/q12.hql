USE team26_projectdb;

DROP TABLE IF EXISTS q12_results;

CREATE EXTERNAL TABLE q12_results(
    column_name STRING,
    nulls       BIGINT,
    total       BIGINT,
    pct_nulls   DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q12';

INSERT INTO q12_results
SELECT 'temperature' AS column_name,
       SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END)           AS nulls,
       COUNT(*)                                                       AS total,
       SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END)/COUNT(*)  AS pct_nulls
FROM traffic_partitioned
UNION ALL
SELECT 'humidity',
       SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END),
       COUNT(*),
       SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END)/COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'pressure',
       SUM(CASE WHEN pressure IS NULL THEN 1 ELSE 0 END),
       COUNT(*),
       SUM(CASE WHEN pressure IS NULL THEN 1 ELSE 0 END)/COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'visibility',
       SUM(CASE WHEN visibility IS NULL THEN 1 ELSE 0 END),
       COUNT(*),
       SUM(CASE WHEN visibility IS NULL THEN 1 ELSE 0 END)/COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'wind_speed',
       SUM(CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END),
       COUNT(*),
       SUM(CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END)/COUNT(*)
FROM traffic_partitioned;

INSERT OVERWRITE DIRECTORY 'project/output/q12'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q12_results;
