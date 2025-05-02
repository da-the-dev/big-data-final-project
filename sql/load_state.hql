USE team26_projectdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Insert data for specific state
INSERT INTO TABLE traffic_partitioned 
PARTITION(state='${hivevar:STATE}')
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
    description, street, city, county,
    country, zip_code, local_time_zone, 
    weather_station_airport_code, 
    from_utc_timestamp(
        from_unixtime(CAST(weather_time_stamp/1000 AS BIGINT)), 
        CASE WHEN local_time_zone IS NULL OR local_time_zone = '' THEN 'UTC' ELSE local_time_zone END
    ) AS weather_time_stamp,
    temperature, wind_chill, humidity, pressure, 
    visibility, wind_dir, wind_speed, precipitation,
    weather_event, weather_conditions
FROM traffic
WHERE state = '${hivevar:STATE}';
