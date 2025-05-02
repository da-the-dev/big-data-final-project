USE team26_projectdb;

DROP TABLE IF EXISTS q11_results;

CREATE EXTERNAL TABLE q11_results(
    weather_event  STRING,
    severe_count   BIGINT,
    total_count    BIGINT,
    share_severe   DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q11';

INSERT INTO q11_results
SELECT
    COALESCE(weather_event,'(none)')                          AS weather_event,
    SUM(CASE WHEN severity >= 3 THEN 1 ELSE 0 END)            AS severe_count,
    COUNT(*)                                                  AS total_count,
    SUM(CASE WHEN severity >= 3 THEN 1 ELSE 0 END) / COUNT(*) AS share_severe
FROM traffic_partitioned
GROUP BY COALESCE(weather_event,'(none)')
ORDER BY share_severe DESC;

INSERT OVERWRITE DIRECTORY 'project/output/q11'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q11_results;
