USE team26_projectdb;

DROP TABLE IF EXISTS ${hivevar:RESULT_TABLE};

CREATE EXTERNAL TABLE ${hivevar:RESULT_TABLE} (
    weather_event STRING,
    severe_count BIGINT,
    total_count BIGINT,
    share_severe DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hivevar:WAREHOUSE_PATH}';

INSERT INTO ${hivevar:RESULT_TABLE}
SELECT
    COALESCE(weather_event, '(none)') AS weather_event,
    SUM(CASE WHEN severity >= 3 THEN 1 ELSE 0 END) AS severe_count,
    COUNT(*) AS total_count,
    SUM(CASE WHEN severity >= 3 THEN 1 ELSE 0 END) / COUNT(*) AS share_severe
FROM traffic_partitioned
GROUP BY COALESCE(weather_event, '(none)')
ORDER BY share_severe DESC;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_PATH}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM ${hivevar:RESULT_TABLE};
