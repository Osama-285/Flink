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

    table = (
        t_env.from_path("flight")
        .window(
            Tumble.over(expr.lit(5).seconds)  
                  .on(expr.col("timestamp"))
                  .alias("w")
        )
        .group_by(expr.col("airline"), expr.col("w"))
        .select(
            expr.col("airline"),
            expr.call("COUNT", expr.col("flight")).alias("flight_count"),
            expr.col("w").start.alias("window_start"),
            expr.col("w").end.alias("window_end")
        )
    )

    print("Aggregated stream started ( tumbling window). Press Ctrl+C to stop.")
    try:
        table.execute().print()
    except KeyboardInterrupt:
        print("\nStopping stream...")
    finally:
        t_env.execute_sql("DROP TABLE flight")
        print("Cleaned up resources.")

if __name__ == "__main__":
    main()

