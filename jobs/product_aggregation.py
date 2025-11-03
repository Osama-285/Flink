from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # --- Execution Environment ---
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # --- Kafka Source Table ---
    t_env.execute_sql("""
        CREATE TABLE product_events (
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
            'topic' = 'productEvent',
            'properties.bootstrap.servers' = 'broker:29094',
            'properties.group.id' = 'product-agg-group',
            'scan.startup.mode' = 'earliest-offset',
            'json.timestamp-format.standard' = 'ISO-8601',
            'format' = 'json'
        )
    """)

    # --- Print Sink Table ---
    t_env.execute_sql("""
        CREATE TABLE product_aggregates (
            category STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_quantity BIGINT,
            total_weight DOUBLE,
            delivered_count BIGINT,
            unloaded_count BIGINT,
            avg_weight DOUBLE
        ) WITH (
            'connector' = 'print'
        )
    """)

    # --- Continuous Insert Query (this triggers the job automatically) ---
    t_env.execute_sql("""
        INSERT INTO product_aggregates
        SELECT
            category,
            window_start,
            window_end,
            SUM(quantity) AS total_quantity,
            SUM(weightKg) AS total_weight,
            SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count,
            SUM(CASE WHEN status = 'UNLOADED' THEN 1 ELSE 0 END) AS unloaded_count,
            AVG(weightKg) AS avg_weight
        FROM TABLE(
            TUMBLE(TABLE product_events, DESCRIPTOR(`timestamp`), INTERVAL '10' SECOND)
        )
        GROUP BY category, window_start, window_end
    """)

if __name__ == "__main__":
    main()
