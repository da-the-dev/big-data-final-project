START TRANSACTION;

DROP TABLE IF EXISTS traffic CASCADE;

CREATE TABLE IF NOT EXISTS traffic (
    id VARCHAR(255) NOT NULL,
    severity INTEGER NOT NULL,
    start_lat DECIMAL(10, 6) NOT NULL,
    start_lng DECIMAL(10, 6) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    distance DECIMAL(10, 6) NOT NULL,
    delay_from_typical_traffic DECIMAL(10) NOT NULL,
    delay_from_free_flow_speed DECIMAL(10) NOT NULL,
    congestion_speed VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    street VARCHAR(255) NOT NULL,
    city VARCHAR(255) NOT NULL,
    county VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL,
    country VARCHAR(255) NOT NULL,
    zip_code VARCHAR(255) NOT NULL,
    local_time_zone VARCHAR(255) NOT NULL,
    weather_station_airport_code VARCHAR(255) NOT NULL,
    weather_time_stamp TIMESTAMP NOT NULL,
    temperature DECIMAL(10, 6) NOT NULL,
    wind_chill DECIMAL(10, 6) NOT NULL,
    humidity DECIMAL(10, 6) NOT NULL,
    pressure DECIMAL(10, 6) NOT NULL,
    visibility DECIMAL(10, 6) NOT NULL,
    wind_dir VARCHAR(255) NOT NULL,
    wind_speed DECIMAL(10, 6) NOT NULL,
    precipitation DECIMAL(10, 6) NOT NULL,
    weather_event VARCHAR(255),
    weather_conditions VARCHAR(255) NOT NULL
);

COMMIT;

