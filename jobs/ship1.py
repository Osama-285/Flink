from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings,expressions as expr
from pyflink.table.expressions import col, call, call_sql

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # ---------- 2) Kafka source table (all STRINGs to avoid parsing issues) ----------
    t_env.execute_sql("""
        CREATE TABLE ship_events (
            shipId STRING,
            imoNumber STRING,
            name STRING,
            flag STRING,
            capacityTEU STRING,
            totalContainers BIGINT,
            eventType STRING,
            berthId STRING,
            arrivalTime STRING,
            departureTime STRING,
            status STRING,
            `timestamp` TIMESTAMP_LTZ(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'shipEvent',                        -- your topic
            'properties.bootstrap.servers' = 'broker:29094', -- your broker
            'properties.group.id' = 'flink-print-group',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'json.timestamp-format.standard' = 'ISO-8601'

        )
    """)

    table = t_env.from_path("ship_events")
    table_with_year = table.add_columns(
        call_sql("EXTRACT(YEAR FROM `timestamp`)").alias("event_year")

    )

    # 4️⃣ Aggregate
    agg_table = (
        table_with_year
        .group_by(col("shipId"), col("event_year"))
        .select(
            col("shipId"),
            col("event_year"),
            call("SUM", col("totalContainers")).alias("total_containers")
        )
    )

    # 5️⃣ Sink to print
    t_env.execute_sql("""
        CREATE TABLE print_sink (
            shipId STRING,
            event_year BIGINT,
            total_containers BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    # 6️⃣ Execute insert
    agg_table.execute_insert("print_sink").wait()

if __name__ == "__main__":
    main()
