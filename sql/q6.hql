USE team26_projectdb;

DROP TABLE IF EXISTS q6_results;

CREATE EXTERNAL TABLE q6_results(
    year  INT,
    month INT,
    avg_delay DOUBLE,
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q6';

INSERT INTO q6_results
SELECT
    YEAR(start_time)                           AS year,
    MONTH(start_time)                          AS month,
    AVG(delay_from_typical_traffic)            AS avg_delay,
    COUNT(*)                                   AS count
FROM traffic_partitioned
GROUP BY YEAR(start_time), MONTH(start_time)
ORDER BY year, month;

INSERT OVERWRITE DIRECTORY 'project/output/q6'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q6_results;
