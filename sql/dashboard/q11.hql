USE team26_projectdb;

DROP TABLE IF EXISTS ${hivevar:RESULT_TABLE};

CREATE EXTERNAL TABLE ${hivevar:RESULT_TABLE} (
    lat_bucket DOUBLE,
    lng_bucket DOUBLE,
    delay_from_typical_traffic DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hivevar:WAREHOUSE_PATH}';

INSERT INTO ${hivevar:RESULT_TABLE}
SELECT
    FLOOR(start_lat * 10) / 10.0 AS lat_bucket,
    FLOOR(start_lng * 10) / 10.0 AS lng_bucket,
    delay_from_typical_traffic
FROM traffic_partitioned;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_PATH}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM ${hivevar:RESULT_TABLE};
