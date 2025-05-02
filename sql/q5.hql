USE team26_projectdb;

DROP TABLE IF EXISTS q5_results;

CREATE EXTERNAL TABLE q5_results(
    day_of_week INT,        -- 1 = Sunday … 7 = Saturday
    avg_delay   DOUBLE,
    count       BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q5';

INSERT INTO q5_results
SELECT
    DAYOFWEEK(start_time)                      AS day_of_week,
    AVG(delay_from_typical_traffic)            AS avg_delay,
    COUNT(*)                                   AS count
FROM traffic_partitioned
GROUP BY DAYOFWEEK(start_time)
ORDER BY day_of_week;

INSERT OVERWRITE DIRECTORY 'project/output/q5'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q5_results;
