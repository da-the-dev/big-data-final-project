USE team26_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE EXTERNAL TABLE q1_results(
    severity INT,
    avg_delay DOUBLE,
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q1';

INSERT INTO q1_results
SELECT 
    severity,
    AVG(delay_from_typical_traffic)          AS avg_delay,
    COUNT(*)                                 AS count
FROM traffic_partitioned
GROUP BY severity
ORDER BY severity;

INSERT OVERWRITE DIRECTORY 'project/output/q1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q1_results;
