USE team26_projectdb;

DROP TABLE IF EXISTS ${hivevar:RESULT_TABLE};

CREATE EXTERNAL TABLE ${hivevar:RESULT_TABLE} (
    delay_from_typical_traffic DOUBLE,
    distance                   DOUBLE,
    precipitation              DOUBLE,
    temperature                DOUBLE,
    humidity                   DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hivevar:WAREHOUSE_PATH}';

INSERT INTO ${hivevar:RESULT_TABLE}
SELECT
    delay_from_typical_traffic,
    distance,
    precipitation,
    temperature,
    humidity
FROM traffic_partitioned
WHERE
    delay_from_typical_traffic IS NOT NULL AND
    distance IS NOT NULL AND
    precipitation IS NOT NULL AND
    temperature IS NOT NULL AND
    humidity IS NOT NULL;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_PATH}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM ${hivevar:RESULT_TABLE};
