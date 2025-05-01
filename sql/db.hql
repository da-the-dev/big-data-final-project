DROP DATABASE IF EXISTS team26_projectdb CASCADE;

CREATE DATABASE team26_projectdb LOCATION "project/hive/warehouse";
USE team26_projectdb;

SET hive.parquet.timestamp.skip.conversion=true;
SET parquet.int64.timestamp.unit=MILLIS;

-- Create external table for traffic data
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


CREATE EXTERNAL TABLE traffic_partitioned (
    id STRING,
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
PARTITIONED BY (state STRING, severity INT)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/traffic_partitioned';

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE traffic_partitioned 
PARTITION(state, severity)
SELECT 
    id, start_lat, start_lng, 
    CAST(from_unixtime(start_time/1000)) as start_time,
    CAST(from_unixtime(end_time/1000))  s end_time,
    distance, delay_from_typical_traffic, 
    delay_from_free_flow_speed, congestion_speed,
    description, street, city, county, 
    zip_code, local_time_zone, 
    weather_station_airport_code, 
    CAST(from_unixtime(start_time/1000)) as weather_time_stamp,
    temperature, wind_chill, humidity, pressure, 
    visibility, wind_dir, wind_speed, precipitation,
    weather_event, weather_conditions, 
    state, severity
FROM traffic;