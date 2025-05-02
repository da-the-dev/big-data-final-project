USE team26_projectdb;

DROP TABLE IF EXISTS q10_results;

CREATE EXTERNAL TABLE q10_results(
    severity INT,
    avg_duration DOUBLE,   -- в минутах
    avg_delay    DOUBLE,
    count  BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q10';

INSERT INTO q10_results
SELECT
    severity,
    AVG( (unix_timestamp(end_time) - unix_timestamp(start_time))/60.0 )  AS avg_duration,
    AVG(delay_from_typical_traffic)                                      AS avg_delay,
    COUNT(*)                                                             AS count
FROM traffic_partitioned
WHERE end_time IS NOT NULL AND start_time IS NOT NULL
GROUP BY severity
ORDER BY severity;

INSERT OVERWRITE DIRECTORY 'project/output/q10'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q10_results;
