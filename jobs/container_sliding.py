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
    CREATE TABLE event_throughput_advanced (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        eventType STRING,
        total_events BIGINT,
        is_spike BOOLEAN
    ) WITH (
        'connector' = 'print'
    )                                              
    """)

    t_env.execute_sql("""
    INSERT INTO event_throughput_advanced
    SELECT
        window_start,
        window_end,
        eventType,
        COUNT(*) AS total_events,
        CASE WHEN COUNT(*) > 500 THEN TRUE ELSE FALSE END AS is_spike
    FROM TABLE(
        HOP(TABLE container_events, DESCRIPTOR(`timestamp`), INTERVAL '10' SECOND, INTERVAL '1' MINUTE)
    )
    GROUP BY window_start, window_end, eventType        
    """)

if __name__ == "__main__":
    main()