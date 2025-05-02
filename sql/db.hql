DROP DATABASE IF EXISTS team26_projectdb CASCADE;
CREATE DATABASE team26_projectdb LOCATION "project/hive/warehouse";
USE team26_projectdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
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

CREATE TABLE states_list 
STORED AS ORC AS
SELECT DISTINCT state 
FROM traffic 
WHERE state IS NOT NULL AND state != '';

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
