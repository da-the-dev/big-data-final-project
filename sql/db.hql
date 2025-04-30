DROP DATABASE IF EXISTS team26_projectdb CASCADE;

CREATE DATABASE team26_projectdb LOCATION "project/hive/warehouse";
USE team26_projectdb;

-- CREATE EXTERNAL TABLE accidents STORED AS AVRO LOCATION 'project/warehouse/accidents' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/accidents.avsc');

CREATE EXTERNAL TABLE accidents (
    id STRING,
    severity INTEGER,
    start_lat DOUBLE,
    start_lng DOUBLE,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    distance DOUBLE,
    delay_from_typical_traffic DECIMAL(10),
    delay_from_free_flow_speed DECIMAL(10),
    congestion_speed STRING,
    description STRING,
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
    street STRING,
    city STRING,
    country STRING,
    county STRING,
    zip_code STRING,
    local_time_zone STRING,
    weather_station_airport_code STRING,
    weather_conditions STRING
) 
    PARTITIONED BY (
        state_part STRING
    )
    CLUSTERED BY (id) INTO 100 BUCKETS
    STORED AS AVRO LOCATION 'project/warehouse/accidents'
    TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/accidents.avsc', 'avro.compress'='snappy');


SELECT * FROM accidents;