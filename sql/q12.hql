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

-- общее кол-во строк
WITH totals AS (
    SELECT COUNT(*) AS total_rows FROM traffic_partitioned
)
INSERT INTO q12_results
SELECT * FROM (
    SELECT 'temperature'           AS column_name,
           SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) AS nulls,
           t.total_rows                                           AS total,
           SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END)/t.total_rows AS pct_nulls
    FROM traffic_partitioned, totals t
    UNION ALL
    SELECT 'humidity',
           SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END),
           t.total_rows,
           SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END)/t.total_rows
    FROM traffic_partitioned, totals t
    UNION ALL
    SELECT 'pressure',
           SUM(CASE WHEN pressure IS NULL THEN 1 ELSE 0 END),
           t.total_rows,
           SUM(CASE WHEN pressure IS NULL THEN 1 ELSE 0 END)/t.total_rows
    FROM traffic_partitioned, totals t
    UNION ALL
    SELECT 'visibility',
           SUM(CASE WHEN visibility IS NULL THEN 1 ELSE 0 END),
           t.total_rows,
           SUM(CASE WHEN visibility IS NULL THEN 1 ELSE 0 END)/t.total_rows
    FROM traffic_partitioned, totals t
    UNION ALL
    SELECT 'wind_speed',
           SUM(CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END),
           t.total_rows,
           SUM(CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END)/t.total_rows
    FROM traffic_partitioned, totals t
) x;

INSERT OVERWRITE DIRECTORY 'project/output/q12'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q12_results;
