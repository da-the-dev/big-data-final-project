USE team26_projectdb;

DROP TABLE IF EXISTS q7_results;

CREATE EXTERNAL TABLE q7_results(
    state STRING,
    avg_delay DOUBLE,
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q7';

INSERT INTO q7_results
SELECT
    state,
    AVG(delay_from_typical_traffic)            AS avg_delay,
    COUNT(*)                                   AS count
FROM traffic_partitioned
GROUP BY state
ORDER BY avg_delay DESC;

INSERT OVERWRITE DIRECTORY 'project/output/q7'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q7_results;
