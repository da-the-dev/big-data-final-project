USE team26_projectdb;

DROP TABLE IF EXISTS q3_results;

CREATE EXTERNAL TABLE q3_results(
    hour_of_day INT,
    avg_delay DOUBLE,
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q3';

INSERT INTO q3_results
SELECT 
    HOUR(start_time) AS hour_of_day,
    AVG(delay_from_typical_traffic) AS avg_delay,
    COUNT(*) AS count
FROM traffic
GROUP BY HOUR(start_time)
ORDER BY hour_of_day;

INSERT OVERWRITE DIRECTORY 'project/output/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q3_results;
