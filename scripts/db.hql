DROP DATABASE IF EXISTS team26_projectdb;

CREATE DATABASE team26_projectdb LOCATION "project/hive/warehouse";
USE team26_projectdb;

-- CREATE EXTERNAL TABLE accidents STORED AS AVRO LOCATION 'project/warehouse/accidents' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/accidents.avsc');

CREATE EXTERNAL TABLE accidents (
    id VARCHAR(255) NOT NULL,
    severity INTEGER NOT NULL,
    start_lat DECIMAL(10, 6) NOT NULL,
    start_lng DECIMAL(10, 6) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    distance DECIMAL(10, 6) NOT NULL,
    delay_from_typical_traffic DECIMAL(10) NOT NULL,
    delay_from_free_flow_speed DECIMAL(10) NOT NULL,
    congestion_speed DECIMAL(10, 6) NOT NULL,
    description TEXT NOT NULL,
    weather_time_stamp TIMESTAMP NOT NULL,
    temperature DECIMAL(10, 6) NOT NULL,
    wind_chill DECIMAL(10, 6) NOT NULL,
    humidity DECIMAL(10, 6) NOT NULL,
    pressure DECIMAL(10, 6) NOT NULL,
    visibility DECIMAL(10, 6) NOT NULL,
    wind_dir VARCHAR(255) NOT NULL,
    wind_speed DECIMAL(10, 6) NOT NULL,
    precipitation DECIMAL(10, 6) NOT NULL,
    weather_event VARCHAR(255) NOT NULL
) 
    PARTITIONED BY (
        street STRING,
        city STRING,
        county STRING,
        state STRING,
        country STRING,
        zip_code STRING,
        local_time_zone STRING,
        weather_station_airport_code STRING,
        weather_conditions STRING
    )
    CLUSTERED BY (distance) INTO 100 BUCKETS
    STORED AS AVRO LOCATION 'project/warehouse/accidents'
--    TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/accidents.avsc', 'avro.compress'='snappy');
    TBLPROPERTIES ('avro.compress'='snappy');


SELECT * FROM accidents;