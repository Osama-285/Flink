from pyflink.table import EnvironmentSettings, TableEnvironment,expressions as expr
from pyflink.table.window import Tumble  


def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    t_env.execute_sql("""
        CREATE TABLE flight (
            flight STRING,
            airline STRING,
            altitude INT,
            speed INT,
            status STRING,
            `timestamp` TIMESTAMP_LTZ(3),
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'flight',
            'properties.bootstrap.servers' = 'broker:29094',
            'properties.group.id' = 'flight-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    result = t_env.execute_sql("""
        SELECT airline, COUNT(*) AS flight_count, 
                window_start, window_end
        FROM TABLE(
            TUMBLE(TABLE flight, DESCRIPTOR(`timestamp`), INTERVAL '5' SECOND)
        )
        GROUP BY airline, window_start, window_end
        """)
    result.print()

if __name__ == "__main__":
    main()

