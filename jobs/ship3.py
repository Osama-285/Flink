from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, expressions as expr
from pyflink.table.window import Tumble
from pyflink.table.expressions import col, call_sql
import time


def main():
    # -------------------- Environment --------------------
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # -------------------- Source Table --------------------
    t_env.execute_sql("""
    CREATE TABLE total_containers (
        shipId STRING,
        imoNumber STRING,
        name STRING,
        flag STRING,
        capacityTEU INT,
        totalContainers INT,
        eventType STRING,
        berthId STRING,
        arrivalTime TIMESTAMP_LTZ(3),
        departureTime TIMESTAMP_LTZ(3),
        status STRING,
        `timestamp` TIMESTAMP_LTZ(3),
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '60' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'shipEvent',
        'properties.bootstrap.servers' = 'broker:29092',
        'properties.group.id' = 'flink_consumer',
        'scan.startup.mode' = 'earliest-offset',
        'json.timestamp-format.standard' = 'ISO-8601',
        'format' = 'json'
    )
    """)

    # -------------------- Create Tables -----------
    table = t_env.from_path("total_containers")
    table_with_year = table.add_columns(
        call_sql("DATE_FORMAT(`timestamp`, 'yyyy')").cast(DataTypes.INT()).alias("event_year")
    )

    table1 = (
        table.window(Tumble.over(expr.lit(60).seconds).on(expr.col("timestamp")).alias("w"))
        .group_by(expr.col("shipId"), expr.col("w"))
        .select(
            expr.col("shipId"),
            expr.call("SUM", expr.col("totalContainers")).alias("total_container"),
            expr.col("w").start.alias("window_start"),
            expr.col("w").end.alias("window_end")
        )
    )

    table2 = (
        table_with_year.window(Tumble.over(expr.lit(30).seconds).on(expr.col("timestamp")).alias("w"))
        .group_by(expr.col("shipId"), col("event_year"), expr.col("w"))
        .select(
            expr.col("shipId"),
            col("event_year").alias("year"),
            expr.call("SUM", expr.col("totalContainers")).alias("total_container"),
            expr.col("w").start.alias("window_start"),
            expr.col("w").end.alias("window_end")
        )
    )

    # create print sinks (same as your code)
    t_env.execute_sql("""
        CREATE TABLE print_sink1 (
            shipId STRING,
            total_container INT,
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3)
        ) WITH ('connector' = 'print')
    """)
    t_env.execute_sql("""
        CREATE TABLE print_sink2 (
            shipId STRING,
            event_year INT,
            total_container INT,
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3)
        ) WITH ('connector' = 'print')
    """)

    print("🚀 Submitting both aggregations (non-blocking)...")

    # submit non-blocking (wait=False)
    table1.execute_insert("print_sink1", False)
    table2.execute_insert("print_sink2", False)

    print("✅ Jobs submitted. Flink cluster is executing both in one job graph.")
    print("Press Ctrl+C to stop and cleanup.")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nStopping...")

    # cleanup optional
    t_env.execute_sql("DROP TABLE total_containers")


if __name__ == "__main__":
    main()
