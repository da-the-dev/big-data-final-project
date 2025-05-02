USE team26_projectdb;

DROP TABLE IF EXISTS q4_results;

CREATE EXTERNAL TABLE q4_results(
    state STRING,
    city  STRING,
    avg_delay DOUBLE,
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q4';

INSERT INTO q4_results
SELECT 
    state,
    city,
    AVG(delay_from_typical_traffic)          AS avg_delay,
    COUNT(*)                                 AS count
FROM traffic_partitioned
WHERE city IS NOT NULL AND city <> ''
GROUP BY state, city
HAVING COUNT(*) > 100
ORDER BY avg_delay DESC
LIMIT 20;

INSERT OVERWRITE DIRECTORY 'project/output/q4'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q4_results;
