USE team26_projectdb;

DROP TABLE IF EXISTS q13_results;

CREATE EXTERNAL TABLE q13_results(
    lat_bucket DOUBLE,
    lng_bucket DOUBLE,
    avg_delay  DOUBLE,
    count      BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q13';

-- округляем координаты до 0.1 градуса (~11 км)
INSERT INTO q13_results
SELECT
    FLOOR(start_lat * 10) / 10.0      AS lat_bucket,
    FLOOR(start_lng * 10) / 10.0      AS lng_bucket,
    AVG(delay_from_typical_traffic)   AS avg_delay,
    COUNT(*)                          AS count
FROM traffic_partitioned
GROUP BY FLOOR(start_lat * 10) / 10.0,
         FLOOR(start_lng * 10) / 10.0;

INSERT OVERWRITE DIRECTORY 'project/output/q13'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q13_results;
