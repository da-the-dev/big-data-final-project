USE team26_projectdb;

DROP TABLE IF EXISTS q15_results;

CREATE EXTERNAL TABLE q15_results(
    year      INT,
    state     STRING,
    avg_delay DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q15';

INSERT INTO q15_results
SELECT
    YEAR(start_time)                       AS year,
    state,
    AVG(delay_from_typical_traffic)        AS avg_delay
FROM traffic_partitioned
GROUP BY YEAR(start_time), state
ORDER BY year, avg_delay DESC;

INSERT OVERWRITE DIRECTORY 'project/output/q15'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM q15_results;
