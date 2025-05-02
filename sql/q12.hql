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
SELECT 'start_time' AS column_name,
       COUNT(*) - COUNT(start_time) AS nulls,
       COUNT(*) AS total,
       (COUNT(*) - COUNT(start_time)) / CAST(COUNT(*) AS DOUBLE) AS pct_nulls
FROM traffic_partitioned
UNION ALL
SELECT 'end_time',
       COUNT(*) - COUNT(end_time),
       COUNT(*),
       (COUNT(*) - COUNT(end_time)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'distance',
       COUNT(*) - COUNT(distance),
       COUNT(*),
       (COUNT(*) - COUNT(distance)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'delay_from_typical_traffic',
       COUNT(*) - COUNT(delay_from_typical_traffic),
       COUNT(*),
       (COUNT(*) - COUNT(delay_from_typical_traffic)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'delay_from_free_flow_speed',
       COUNT(*) - COUNT(delay_from_free_flow_speed),
       COUNT(*),
       (COUNT(*) - COUNT(delay_from_free_flow_speed)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'congestion_speed',
       COUNT(*) - COUNT(congestion_speed),
       COUNT(*),
       (COUNT(*) - COUNT(congestion_speed)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'description',
       COUNT(*) - COUNT(description),
       COUNT(*),
       (COUNT(*) - COUNT(description)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'street',
       COUNT(*) - COUNT(street),
       COUNT(*),
       (COUNT(*) - COUNT(street)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'city',
       COUNT(*) - COUNT(city),
       COUNT(*),
       (COUNT(*) - COUNT(city)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'county',
       COUNT(*) - COUNT(county),
       COUNT(*),
       (COUNT(*) - COUNT(county)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'state',
       COUNT(*) - COUNT(state),
       COUNT(*),
       (COUNT(*) - COUNT(state)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'country',
       COUNT(*) - COUNT(country),
       COUNT(*),
       (COUNT(*) - COUNT(country)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'zip_code',
       COUNT(*) - COUNT(zip_code),
       COUNT(*),
       (COUNT(*) - COUNT(zip_code)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'local_time_zone',
       COUNT(*) - COUNT(local_time_zone),
       COUNT(*),
       (COUNT(*) - COUNT(local_time_zone)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'weather_station_airport_code',
       COUNT(*) - COUNT(weather_station_airport_code),
       COUNT(*),
       (COUNT(*) - COUNT(weather_station_airport_code)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'weather_time_stamp',
       COUNT(*) - COUNT(weather_time_stamp),
       COUNT(*),
       (COUNT(*) - COUNT(weather_time_stamp)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'temperature',
       COUNT(*) - COUNT(temperature),
       COUNT(*),
       (COUNT(*) - COUNT(temperature)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'wind_chill',
       COUNT(*) - COUNT(wind_chill),
       COUNT(*),
       (COUNT(*) - COUNT(wind_chill)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'humidity',
       COUNT(*) - COUNT(humidity),
       COUNT(*),
       (COUNT(*) - COUNT(humidity)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'pressure',
       COUNT(*) - COUNT(pressure),
       COUNT(*),
       (COUNT(*) - COUNT(pressure)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'visibility',
       COUNT(*) - COUNT(visibility),
       COUNT(*),
       (COUNT(*) - COUNT(visibility)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'wind_dir',
       COUNT(*) - COUNT(wind_dir),
       COUNT(*),
       (COUNT(*) - COUNT(wind_dir)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'wind_speed',
       COUNT(*) - COUNT(wind_speed),
       COUNT(*),
       (COUNT(*) - COUNT(wind_speed)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'precipitation',
       COUNT(*) - COUNT(precipitation),
       COUNT(*),
       (COUNT(*) - COUNT(precipitation)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'weather_event',
       COUNT(*) - COUNT(weather_event),
       COUNT(*),
       (COUNT(*) - COUNT(weather_event)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned
UNION ALL
SELECT 'weather_conditions',
       COUNT(*) - COUNT(weather_conditions),
       COUNT(*),
       (COUNT(*) - COUNT(weather_conditions)) / CAST(COUNT(*) AS DOUBLE)
FROM traffic_partitioned;

INSERT OVERWRITE DIRECTORY 'project/output/q12'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q12_results;
