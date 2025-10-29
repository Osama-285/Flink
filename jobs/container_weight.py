from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    t_env.execute_sql("""
    CREATE TABLE container_events (
        containerId STRING,
        shipId STRING,
        size STRING,
        weightKg DOUBLE,
        sealNumber STRING,
        eventType STRING,
        location STRING,
        status STRING,
        `timestamp` TIMESTAMP_LTZ(3),
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '10' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'containerEvent',
        'properties.bootstrap.servers' = 'broker:29094',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'scan.startup.mode' = 'earliest-offset'
    )
    """)

    t_env.execute_sql("""
    CREATE TABLE weight_distribution_advanced (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        shipId STRING,
        location STRING,
        avg_weight DOUBLE,
        max_weight DOUBLE,
        min_weight DOUBLE
    ) WITH (
        'connector' = 'print'
    )            
    """)

    t_env.execute_sql("""
    INSERT INTO weight_distribution_advanced
    SELECT
        window_start,
        window_end,
        shipId,
        location,
        AVG(weightKg) AS avg_weight,
        MAX(weightKg) AS max_weight,
        MIN(weightKg) AS min_weight
    FROM TABLE(
        SESSION(TABLE container_events, DESCRIPTOR(`timestamp`), INTERVAL '2' MINUTE)
    )
    GROUP BY window_start, window_end, shipId, location;      
    """)

if __name__ == "__main__":
    main()