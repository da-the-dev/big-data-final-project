USE team26_projectdb;

DROP TABLE IF EXISTS ${hivevar:RESULT_TABLE};

CREATE EXTERNAL TABLE ${hivevar:RESULT_TABLE} (
    distance_bucket STRING,
    delay_from_typical_traffic DOUBLE,
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hivevar:WAREHOUSE_PATH}';

INSERT INTO ${hivevar:RESULT_TABLE}
SELECT
    CASE
        WHEN distance < 0.5 THEN '<0.5 mi'
        WHEN distance >= 0.5 AND distance < 1 THEN '0.5–1 mi'
        WHEN distance >= 1 AND distance < 3 THEN '1–3 mi'
        ELSE '>3 mi'
    END AS distance_bucket,
    delay_from_typical_traffic,
FROM traffic_partitioned
WHERE distance IS NOT NULL;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_PATH}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM ${hivevar:RESULT_TABLE};
