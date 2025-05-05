USE team26_projectdb;

DROP TABLE IF EXISTS ${hivevar:RESULT_TABLE};

CREATE EXTERNAL TABLE ${hivevar:RESULT_TABLE} (
    column_name STRING,
    nulls BIGINT,
    total BIGINT,
    pct_nulls DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hivevar:WAREHOUSE_PATH}';

DROP TABLE IF EXISTS q12_total_rows;

CREATE TABLE q12_total_rows AS
SELECT COUNT(*) AS total
FROM traffic_partitioned;

INSERT INTO ${hivevar:RESULT_TABLE}
SELECT 'delay_from_typical_traffic',
       SUM(CASE WHEN delay_from_typical_traffic IS NULL THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN delay_from_typical_traffic IS NULL THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'delay_from_free_flow_speed',
       SUM(CASE WHEN delay_from_free_flow_speed IS NULL THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN delay_from_free_flow_speed IS NULL THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'congestion_speed',
       SUM(CASE WHEN congestion_speed IS NULL OR congestion_speed = '' THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN congestion_speed IS NULL OR congestion_speed = '' THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'temperature',
       SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'wind_chill',
       SUM(CASE WHEN wind_chill IS NULL THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN wind_chill IS NULL THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'humidity',
       SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'pressure',
       SUM(CASE WHEN pressure IS NULL THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN pressure IS NULL THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'visibility',
       SUM(CASE WHEN visibility IS NULL THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN visibility IS NULL THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'wind_speed',
       SUM(CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'precipitation',
       SUM(CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'weather_time_stamp',
       SUM(CASE WHEN weather_time_stamp IS NULL THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN weather_time_stamp IS NULL THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'weather_event',
       SUM(CASE WHEN weather_event IS NULL OR weather_event = '' THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN weather_event IS NULL OR weather_event = '' THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'weather_conditions',
       SUM(CASE WHEN weather_conditions IS NULL OR weather_conditions = '' THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN weather_conditions IS NULL OR weather_conditions = '' THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'local_time_zone',
       SUM(CASE WHEN local_time_zone IS NULL OR local_time_zone = '' THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN local_time_zone IS NULL OR local_time_zone = '' THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t
UNION ALL
SELECT 'weather_station_airport_code',
       SUM(CASE WHEN weather_station_airport_code IS NULL OR weather_station_airport_code = '' THEN 1 ELSE 0 END),
       t.total,
       SUM(CASE WHEN weather_station_airport_code IS NULL OR weather_station_airport_code = '' THEN 1 ELSE 0 END) / t.total
FROM traffic_partitioned, q12_total_rows t;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_PATH}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM ${hivevar:RESULT_TABLE};
