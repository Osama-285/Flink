from pyflink.table import EnvironmentSettings, DataTypes,TableEnvironment,expressions as expr
from pyflink.table.window import Tumble
from pyflink.table.expressions import col, call, call_sql


def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

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
        'properties.bootstrap.servers' = 'broker:29094',
        'properties.group.id' = 'flink_consumer',
        'scan.startup.mode' = 'latest-offset',
        'json.timestamp-format.standard' = 'ISO-8601',
        'format' = 'json'
    )
    """)
    table = t_env.from_path("total_containers")
    table_with_year = table.add_columns(
        call_sql("EXTRACT(YEAR FROM `timestamp`)").alias("event_year")

    )
    table1 =(
        table
        .window(
            Tumble.over(expr.lit(60).seconds).on(expr.col("timestamp")).alias("w")
        ).group_by(expr.col('shipId'),expr.col("w"))
        .select(
            expr.col("shipId"),
            expr.call("SUM",expr.col("totalContainers")).alias("total_container"),
            expr.col("w").start.alias("window_start"),
            expr.col("w").end.alias("window_end")
        )
    )
    # year_expr = expr.call("DATE_FORMAT", expr.col("timestamp"), expr.lit("yyyy")).cast(DataTypes.INT())

    table2 =(
        table_with_year
        .window(
            Tumble.over(expr.lit(30).seconds).on(expr.col("timestamp")).alias("w")
        ).group_by(expr.col('shipId'),col("event_year"),expr.col("w"))
        .select(
            expr.col("shipId"),
            col("event_year").alias("year"),
            expr.call("SUM",expr.col("totalContainers")).alias("total_container"),
            expr.col("w").start.alias("window_start"),
            expr.col("w").end.alias("window_end")
        )
    )

    print("Aggregated stream started ( tumbling window). Press Ctrl+C to stop.")
    try:
        print("TABLE1 Executing")
        table1.execute().print()
        print("TABLE2 Executing")
        table2.execute().print()
    except KeyboardInterrupt:
        print("\nStopping stream...")
    finally:
        t_env.execute_sql("DROP TABLE total_containers")
        print("Cleaned up resources.")

if __name__ == "__main__":
    main()