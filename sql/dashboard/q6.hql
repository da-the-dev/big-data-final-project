USE team26_projectdb;

DROP TABLE IF EXISTS ${hivevar:RESULT_TABLE};

CREATE EXTERNAL TABLE ${hivevar:RESULT_TABLE} (
    year INT,
    month INT,
    delay_from_typical_traffic DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hivevar:WAREHOUSE_PATH}';

INSERT INTO ${hivevar:RESULT_TABLE}
SELECT
    YEAR(start_time) AS year,
    MONTH(start_time) AS month,
    delay_from_typical_traffic
FROM traffic_partitioned
WHERE start_time IS NOT NULL;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_PATH}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM ${hivevar:RESULT_TABLE};
