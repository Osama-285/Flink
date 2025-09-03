from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    t_env.execute_sql("""
        CREATE TABLE flights (
            flight STRING,
            airline STRING,
            altitude INT,
            speed INT,
            status STRING,
            event_time TIMESTAMP_LTZ(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'flight',
            'properties.bootstrap.servers' = 'broker:29094',
            'properties.group.id' = 'flink-flight-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    print("Tables:", t_env.list_tables())
    print("Functions:", t_env.list_user_defined_functions())
    print("Current catalog:", t_env.get_current_catalog())
    print("Current database:", t_env.get_current_database())

    t_env.execute_sql("""
    CREATE TEMPORARY TABLE print_sink (
        flight STRING,
        airline STRING,
        altitude INT,
        speed INT,
        status STRING,
        `timestamp` TIMESTAMP_LTZ(3)
    ) WITH (
        'connector' = 'print'
    )
    """)
    print("TABLES",t_env.list_temporary_tables)

    t_env.execute_sql("""
    INSERT INTO print_sink
    SELECT * FROM flights
""")

    # t_env.execute_sql("SELECT * FROM flights")

if __name__ == "__main__":
    main()
