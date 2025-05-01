START TRANSACTION;

DROP TABLE IF EXISTS traffic CASCADE;

CREATE TABLE IF NOT EXISTS traffic (
    id VARCHAR(255) NOT NULL PRIMARY KEY,
    severity INTEGER NOT NULL,
    start_lat DOUBLE PRECISION NOT NULL,
    start_lng DOUBLE PRECISION NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    distance DOUBLE PRECISION,
    delay_from_typical_traffic DOUBLE PRECISION,
    delay_from_free_flow_speed DOUBLE PRECISION,
    congestion_speed VARCHAR(255),
    description TEXT,
    street VARCHAR(255),
    city VARCHAR(255),
    county VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    zip_code VARCHAR(255),
    local_time_zone VARCHAR(255),
    weather_station_airport_code VARCHAR(255),
    weather_time_stamp TIMESTAMP,
    temperature DOUBLE PRECISION,
    wind_chill DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    visibility DOUBLE PRECISION,
    wind_dir VARCHAR(255),
    wind_speed DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    weather_event VARCHAR(255),
    weather_conditions VARCHAR(255)
);

COMMIT;
