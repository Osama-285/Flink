from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# -------------------------------
# Configuration
# -------------------------------
KAFKA_BROKERS = "broker:29094"
KAFKA_TOPIC = "productEvent"

def main():
    # -------------------------------
    # Environment Setup (Flink 2.0)
    # -------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(10_000)  # 10s interval for fault-tolerance
    env.get_config().set_auto_watermark_interval(2000)  # 2s periodic watermark emission

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # -------------------------------
    # Kafka Source Table (Event Time + Watermark)
    # -------------------------------
    t_env.execute_sql(f"""
        CREATE TABLE product_event (
            productId STRING,
            containerId STRING,
            shipId STRING,
            name STRING,
            category STRING,
            quantity INT,
            weightKg DOUBLE,
            status STRING,
            `timestamp` TIMESTAMP_LTZ(3),
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
            'properties.group.id' = 'flink-late-handler',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    # -------------------------------
    # Deduplication (latest event per productId)
    # -------------------------------
    deduped = t_env.sql_query("""
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY productId ORDER BY `timestamp` DESC) AS rownum
            FROM product_event
        )
        WHERE rownum = 1
    """)

    # -------------------------------
    # 1-Minute Event-Time Window Aggregation
    # -------------------------------
    product_agg = t_env.sql_query("""
        SELECT
            category,
            TUMBLE_START(`timestamp`, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(`timestamp`, INTERVAL '1' MINUTE) AS window_end,
            COUNT(*) AS total_products,
            SUM(quantity) AS total_quantity,
            SUM(weightKg) AS total_weight
        FROM product_event
        WHERE category IS NOT NULL
        GROUP BY
            TUMBLE(`timestamp`, INTERVAL '1' MINUTE),
            category
    """)

    # -------------------------------
    # Upsert Sink (Idempotent Output)
    # -------------------------------
    t_env.execute_sql("""
        CREATE TABLE product_agg_sink (
            category STRING,
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            total_products BIGINT,
            total_quantity BIGINT,
            total_weight DOUBLE,
            PRIMARY KEY (category, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'productAgg',
            'properties.bootstrap.servers' = 'broker:29094',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    # Write to sink
    product_agg.execute_insert("product_agg_sink")

    # Debug print (optional)
    t_env.to_changelog_stream(product_agg).print()

    # Execute job
    env.execute("Robust Late-Arrival & Reprocessing Framework (Flink 2.0)")


if __name__ == "__main__":
    main()
