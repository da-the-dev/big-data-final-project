USE team26_projectdb;

DROP TABLE IF EXISTS ${hivevar:RESULT_TABLE};

CREATE EXTERNAL TABLE ${hivevar:RESULT_TABLE} (
    visibility_bucket STRING,
    avg_delay DOUBLE,
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hivevar:WAREHOUSE_PATH}';

INSERT INTO ${hivevar:RESULT_TABLE}
SELECT
    CASE
        WHEN visibility < 1 THEN '<1 mi'
        WHEN visibility >= 1 AND visibility < 3 THEN '1–3 mi'
        WHEN visibility >= 3 AND visibility < 5 THEN '3–5 mi'
        ELSE '>5 mi'
    END AS visibility_bucket,
    AVG(delay_from_typical_traffic) AS avg_delay,
    COUNT(*) AS count
FROM traffic_partitioned
WHERE visibility IS NOT NULL
GROUP BY CASE
    WHEN visibility < 1 THEN '<1 mi'
    WHEN visibility >= 1 AND visibility < 3 THEN '1–3 mi'
    WHEN visibility >= 3 AND visibility < 5 THEN '3–5 mi'
    ELSE '>5 mi'
END
ORDER BY avg_delay DESC;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_PATH}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM ${hivevar:RESULT_TABLE};
