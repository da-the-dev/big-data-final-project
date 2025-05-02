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
SELECT 'start_time', COUNT(*) - COUNT(start_time), COUNT(*), (COUNT(*) - COUNT(start_time)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'end_time', COUNT(*) - COUNT(end_time), COUNT(*), (COUNT(*) - COUNT(end_time)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'distance', COUNT(*) - COUNT(distance), COUNT(*), (COUNT(*) - COUNT(distance)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'delay_from_typical_traffic', COUNT(*) - COUNT(delay_from_typical_traffic), COUNT(*), (COUNT(*) - COUNT(delay_from_typical_traffic)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'delay_from_free_flow_speed', COUNT(*) - COUNT(delay_from_free_flow_speed), COUNT(*), (COUNT(*) - COUNT(delay_from_free_flow_speed)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'congestion_speed', COUNT(*) - COUNT(congestion_speed), COUNT(*), (COUNT(*) - COUNT(congestion_speed)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'description', COUNT(*) - COUNT(description), COUNT(*), (COUNT(*) - COUNT(description)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'street', COUNT(*) - COUNT(street), COUNT(*), (COUNT(*) - COUNT(street)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'city', COUNT(*) - COUNT(city), COUNT(*), (COUNT(*) - COUNT(city)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'county', COUNT(*) - COUNT(county), COUNT(*), (COUNT(*) - COUNT(county)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'state', COUNT(*) - COUNT(state), COUNT(*), (COUNT(*) - COUNT(state)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'country', COUNT(*) - COUNT(country), COUNT(*), (COUNT(*) - COUNT(country)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'zip_code', COUNT(*) - COUNT(zip_code), COUNT(*), (COUNT(*) - COUNT(zip_code)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'local_time_zone', COUNT(*) - COUNT(local_time_zone), COUNT(*), (COUNT(*) - COUNT(local_time_zone)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'weather_station_airport_code', COUNT(*) - COUNT(weather_station_airport_code), COUNT(*), (COUNT(*) - COUNT(weather_station_airport_code)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'weather_time_stamp', COUNT(*) - COUNT(weather_time_stamp), COUNT(*), (COUNT(*) - COUNT(weather_time_stamp)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'temperature', COUNT(*) - COUNT(temperature), COUNT(*), (COUNT(*) - COUNT(temperature)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'wind_chill', COUNT(*) - COUNT(wind_chill), COUNT(*), (COUNT(*) - COUNT(wind_chill)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'humidity', COUNT(*) - COUNT(humidity), COUNT(*), (COUNT(*) - COUNT(humidity)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'pressure', COUNT(*) - COUNT(pressure), COUNT(*), (COUNT(*) - COUNT(pressure)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'visibility', COUNT(*) - COUNT(visibility), COUNT(*), (COUNT(*) - COUNT(visibility)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'wind_dir', COUNT(*) - COUNT(wind_dir), COUNT(*), (COUNT(*) - COUNT(wind_dir)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'wind_speed', COUNT(*) - COUNT(wind_speed), COUNT(*), (COUNT(*) - COUNT(wind_speed)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'precipitation', COUNT(*) - COUNT(precipitation), COUNT(*), (COUNT(*) - COUNT(precipitation)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'weather_event', COUNT(*) - COUNT(weather_event), COUNT(*), (COUNT(*) - COUNT(weather_event)) / COUNT(*)
FROM traffic_partitioned
UNION ALL
SELECT 'weather_conditions', COUNT(*) - COUNT(weather_conditions), COUNT(*), (COUNT(*) - COUNT(weather_conditions)) / COUNT(*)
FROM traffic_partitioned;

INSERT OVERWRITE DIRECTORY 'project/output/q12'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q12_results;
