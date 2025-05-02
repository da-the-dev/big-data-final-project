USE team26_projectdb;

DROP TABLE IF EXISTS q2_results;

CREATE EXTERNAL TABLE q2_results(
    weather_conditions STRING,
    avg_delay DOUBLE,
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q2';

INSERT INTO q2_results
SELECT 
    weather_conditions,
    AVG(delay_from_typical_traffic) AS avg_delay,
    COUNT(*) AS count
FROM traffic
WHERE weather_conditions IS NOT NULL AND weather_conditions != ''
GROUP BY weather_conditions
ORDER BY avg_delay DESC;

INSERT OVERWRITE DIRECTORY 'project/output/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q2_results;
