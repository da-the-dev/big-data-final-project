DROP DATABASE IF EXISTS team26_projectdb CASCADE;

CREATE DATABASE team26_projectdb LOCATION "project/hive/warehouse";
USE team26_projectdb;

-- CREATE EXTERNAL TABLE accidents STORED AS AVRO LOCATION 'project/warehouse/accidents' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/accidents.avsc');

CREATE EXTERNAL TABLE accidents (
    id STRING,
    severity INTEGER,
    start_lat DECIMAL(10, 6),
    start_lng DECIMAL(10, 6),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    distance DECIMAL(10, 6),
    delay_from_typical_traffic DECIMAL(10),
    delay_from_free_flow_speed DECIMAL(10),
    congestion_speed DECIMAL(10, 6),
    description TEXT,
    weather_time_stamp TIMESTAMP,
    temperature DECIMAL(10, 6),
    wind_chill DECIMAL(10, 6),
    humidity DECIMAL(10, 6),
    pressure DECIMAL(10, 6),
    visibility DECIMAL(10, 6),
    wind_dir STRING,
    wind_speed DECIMAL(10, 6),
    precipitation DECIMAL(10, 6),
    weather_event STRING 
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
        state STRING
    )
    CLUSTERED BY (id) INTO 100 BUCKETS
    STORED AS AVRO LOCATION 'project/warehouse/accidents'
    TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/accidents.avsc', 'avro.compress'='snappy');


SELECT * FROM accidents;