USE team26_projectdb;

DROP TABLE IF EXISTS q9_results;

CREATE EXTERNAL TABLE q9_results(
    distance_bucket STRING,
    avg_delay DOUBLE,
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q9';

INSERT INTO q9_results
SELECT
    CASE
        WHEN distance < 0.5                 THEN '<0.5 mi'
        WHEN distance >= 0.5 AND distance < 1   THEN '0.5–1 mi'
        WHEN distance >= 1   AND distance < 3   THEN '1–3 mi'
        ELSE '>3 mi'
    END                                        AS distance_bucket,
    AVG(delay_from_typical_traffic)            AS avg_delay,
    COUNT(*)                                   AS count
FROM traffic_partitioned
WHERE distance IS NOT NULL
GROUP BY CASE
        WHEN distance < 0.5                 THEN '<0.5 mi'
        WHEN distance >= 0.5 AND distance < 1   THEN '0.5–1 mi'
        WHEN distance >= 1   AND distance < 3   THEN '1–3 mi'
        ELSE '>3 mi'
    END
ORDER BY avg_delay DESC;

INSERT OVERWRITE DIRECTORY 'project/output/q9'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q9_results;
