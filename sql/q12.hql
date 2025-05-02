USE team26_projectdb;

DROP TABLE IF EXISTS q12_results;

CREATE EXTERNAL TABLE q12_results(
    column_name STRING,
    nulls       BIGINT,
    total       BIGINT,
    pct_nulls   DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q12';

INSERT INTO q12_results
SELECT 'start_time', SUM(CASE WHEN start_time IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN start_time IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'end_time', SUM(CASE WHEN end_time IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN end_time IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'distance', SUM(CASE WHEN distance IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN distance IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'delay_from_typical_traffic', SUM(CASE WHEN delay_from_typical_traffic IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN delay_from_typical_traffic IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'delay_from_free_flow_speed', SUM(CASE WHEN delay_from_free_flow_speed IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN delay_from_free_flow_speed IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'congestion_speed', SUM(CASE WHEN congestion_speed IS NULL OR congestion_speed = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN congestion_speed IS NULL OR congestion_speed = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'description', SUM(CASE WHEN description IS NULL OR description = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN description IS NULL OR description = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'street', SUM(CASE WHEN street IS NULL OR street = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN street IS NULL OR street = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'city', SUM(CASE WHEN city IS NULL OR city = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN city IS NULL OR city = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'county', SUM(CASE WHEN county IS NULL OR county = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN county IS NULL OR county = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'state', SUM(CASE WHEN state IS NULL OR state = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN state IS NULL OR state = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'country', SUM(CASE WHEN country IS NULL OR country = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN country IS NULL OR country = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'zip_code', SUM(CASE WHEN zip_code IS NULL OR zip_code = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN zip_code IS NULL OR zip_code = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'local_time_zone', SUM(CASE WHEN local_time_zone IS NULL OR local_time_zone = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN local_time_zone IS NULL OR local_time_zone = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'weather_station_airport_code', SUM(CASE WHEN weather_station_airport_code IS NULL OR weather_station_airport_code = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN weather_station_airport_code IS NULL OR weather_station_airport_code = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'weather_time_stamp', SUM(CASE WHEN weather_time_stamp IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN weather_time_stamp IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'temperature', SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'wind_chill', SUM(CASE WHEN wind_chill IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN wind_chill IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'humidity', SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'pressure', SUM(CASE WHEN pressure IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN pressure IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'visibility', SUM(CASE WHEN visibility IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN visibility IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'wind_dir', SUM(CASE WHEN wind_dir IS NULL OR wind_dir = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN wind_dir IS NULL OR wind_dir = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'wind_speed', SUM(CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'precipitation', SUM(CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'weather_event', SUM(CASE WHEN weather_event IS NULL OR weather_event = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN weather_event IS NULL OR weather_event = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
UNION ALL
SELECT 'weather_conditions', SUM(CASE WHEN weather_conditions IS NULL OR weather_conditions = '' THEN 1 ELSE 0 END), COUNT(*), SUM(CASE WHEN weather_conditions IS NULL OR weather_conditions = '' THEN 1 ELSE 0 END)/COUNT(*) FROM traffic_partitioned
;

INSERT OVERWRITE DIRECTORY 'project/output/q12'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q12_results;
