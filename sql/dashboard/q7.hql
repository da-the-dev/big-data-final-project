USE team26_projectdb;

DROP TABLE IF EXISTS ${hivevar:RESULT_TABLE};

CREATE EXTERNAL TABLE ${hivevar:RESULT_TABLE} (
    state STRING,
    delay_from_typical_traffic DOUBLE,
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hivevar:WAREHOUSE_PATH}';

INSERT INTO ${hivevar:RESULT_TABLE}
SELECT
    state,
    delay_from_typical_traffic,
FROM traffic_partitioned;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_PATH}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM ${hivevar:RESULT_TABLE};
