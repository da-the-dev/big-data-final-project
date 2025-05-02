DROP DATABASE IF EXISTS team26_projectdb CASCADE;

CREATE DATABASE team26_projectdb LOCATION "project/hive/warehouse";
USE team26_projectdb;

SET hive.parquet.timestamp.skip.conversion=true;
SET parquet.int64.timestamp.unit=MILLIS;
SET hive.local.time.zone=UTC; 
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.bucket.pruning=true;
SET hive.optimize.sort.dynamic.partition=true;

CREATE EXTERNAL TABLE traffic (
  id STRING,
  severity INT,
  start_lat DOUBLE,
  start_lng DOUBLE,
  start_time BIGINT,
  end_time BIGINT,
  distance DOUBLE,
  delay_from_typical_traffic DOUBLE,
  delay_from_free_flow_speed DOUBLE,
  congestion_speed STRING,
  description STRING,
  street STRING,
  city STRING,
  county STRING,
  state STRING,
  country STRING,
  zip_code STRING,
  local_time_zone STRING,
  weather_station_airport_code STRING,
  weather_time_stamp BIGINT,
  temperature DOUBLE,
  wind_chill DOUBLE,
  humidity DOUBLE,
  pressure DOUBLE,
  visibility DOUBLE,
  wind_dir STRING,
  wind_speed DOUBLE,
  precipitation DOUBLE,
  weather_event STRING,
  weather_conditions STRING
)
STORED AS PARQUET
LOCATION 'project/warehouse/traffic';

CREATE VIEW traffic_view AS
SELECT 
    id, severity, start_lat, start_lng, 
    from_utc_timestamp(
        from_unixtime(CAST(start_time/1000 AS BIGINT)), 
        CASE WHEN local_time_zone IS NULL OR local_time_zone = '' THEN 'UTC' ELSE local_time_zone END
    ) AS start_time,
    from_utc_timestamp(
        from_unixtime(CAST(end_time/1000 AS BIGINT)), 
        CASE WHEN local_time_zone IS NULL OR local_time_zone = '' THEN 'UTC' ELSE local_time_zone END
    ) AS end_time,
    distance, delay_from_typical_traffic, 
    delay_from_free_flow_speed, congestion_speed,
    description, street, city, county, state,
    country, zip_code, local_time_zone, 
    weather_station_airport_code, 
    from_utc_timestamp(
        from_unixtime(CAST(weather_time_stamp/1000 AS BIGINT)), 
        CASE WHEN local_time_zone IS NULL OR local_time_zone = '' THEN 'UTC' ELSE local_time_zone END
    ) AS weather_time_stamp,
    temperature, wind_chill, humidity, pressure, 
    visibility, wind_dir, wind_speed, precipitation,
    weather_event, weather_conditions
FROM traffic;


CREATE EXTERNAL TABLE traffic_partitioned (
    id STRING,
    severity INT,
    start_lat DOUBLE,
    start_lng DOUBLE,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    distance DOUBLE,
    delay_from_typical_traffic DOUBLE,
    delay_from_free_flow_speed DOUBLE,
    congestion_speed STRING,
    description STRING,
    street STRING,
    city STRING,
    county STRING,
    country STRING,
    zip_code STRING,
    local_time_zone STRING,
    weather_station_airport_code STRING,
    weather_time_stamp TIMESTAMP,
    temperature DOUBLE,
    wind_chill DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE,
    visibility DOUBLE,
    wind_dir STRING,
    wind_speed DOUBLE,
    precipitation DOUBLE,
    weather_event STRING,
    weather_conditions STRING
)
PARTITIONED BY (state STRING)
CLUSTERED BY (city) INTO 4 BUCKETS
STORED AS PARQUET
LOCATION 'project/hive/warehouse/traffic_partitioned';

INSERT OVERWRITE TABLE traffic_partitioned 
PARTITION(state)
SELECT 
    id, severity, start_lat, start_lng, 
    start_time, end_time,
    distance, delay_from_typical_traffic, 
    delay_from_free_flow_speed, congestion_speed,
    description, street, city, county,
    country, zip_code, local_time_zone, 
    weather_station_airport_code, weather_time_stamp,
    temperature, wind_chill, humidity, pressure, 
    visibility, wind_dir, wind_speed, precipitation,
    weather_event, weather_conditions, state
FROM traffic_view;