USE team26_projectdb;

DROP TABLE IF EXISTS ${hivevar:RESULT_TABLE};

CREATE EXTERNAL TABLE ${hivevar:RESULT_TABLE} (
    column_name STRING,
    nulls BIGINT,
    total BIGINT,
    pct_nulls DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hivevar:WAREHOUSE_PATH}';

INSERT INTO ${hivevar:RESULT_TABLE}
SELECT
    q.column_name,
    q.nulls,
    t.total,
    IF(t.total = 0, 0.0, q.nulls / t.total) AS pct_nulls
FROM (
    SELECT 'delay_from_typical_traffic' AS column_name,
           COUNT(*) FILTER (WHERE delay_from_typical_traffic IS NULL) AS nulls
    FROM traffic_partitioned

    UNION ALL

    SELECT 'delay_from_free_flow_speed',
           COUNT(*) FILTER (WHERE delay_from_free_flow_speed IS NULL)
    FROM traffic_partitioned

    UNION ALL

    SELECT 'congestion_speed',
           COUNT(*) FILTER (WHERE congestion_speed IS NULL OR congestion_speed = '')
    FROM traffic_partitioned

    UNION ALL

    SELECT 'temperature',
           COUNT(*) FILTER (WHERE temperature IS NULL)
    FROM traffic_partitioned

    UNION ALL

    SELECT 'wind_chill',
           COUNT(*) FILTER (WHERE wind_chill IS NULL)
    FROM traffic_partitioned

    UNION ALL

    SELECT 'humidity',
           COUNT(*) FILTER (WHERE humidity IS NULL)
    FROM traffic_partitioned

    UNION ALL

    SELECT 'pressure',
           COUNT(*) FILTER (WHERE pressure IS NULL)
    FROM traffic_partitioned

    UNION ALL

    SELECT 'visibility',
           COUNT(*) FILTER (WHERE visibility IS NULL)
    FROM traffic_partitioned

    UNION ALL

    SELECT 'wind_speed',
           COUNT(*) FILTER (WHERE wind_speed IS NULL)
    FROM traffic_partitioned

    UNION ALL

    SELECT 'precipitation',
           COUNT(*) FILTER (WHERE precipitation IS NULL)
    FROM traffic_partitioned

    UNION ALL

    SELECT 'weather_time_stamp',
           COUNT(*) FILTER (WHERE weather_time_stamp IS NULL)
    FROM traffic_partitioned

    UNION ALL

    SELECT 'weather_event',
           COUNT(*) FILTER (WHERE weather_event IS NULL OR weather_event = '')
    FROM traffic_partitioned

    UNION ALL

    SELECT 'weather_conditions',
           COUNT(*) FILTER (WHERE weather_conditions IS NULL OR weather_conditions = '')
    FROM traffic_partitioned

    UNION ALL

    SELECT 'local_time_zone',
           COUNT(*) FILTER (WHERE local_time_zone IS NULL OR local_time_zone = '')
    FROM traffic_partitioned

    UNION ALL

    SELECT 'weather_station_airport_code',
           COUNT(*) FILTER (WHERE weather_station_airport_code IS NULL OR weather_station_airport_code = '')
    FROM traffic_partitioned
) q
CROSS JOIN (
    SELECT COUNT(*) AS total FROM traffic_partitioned
) t;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_PATH}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM ${hivevar:RESULT_TABLE};
