# flink_product_hierarchy_kafka.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# ---------------------------------------------------------
#  Configuration
# ---------------------------------------------------------
KAFKA_BROKERS = "broker:29094"          # Your Confluent broker from Docker
KAFKA_TOPIC = "productEvent"
KAFKA_GROUP_ID = "flink_product_consumer"

# ---------------------------------------------------------
#  Main Job
# ---------------------------------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Optimize for low-latency + incremental aggregation
    conf = t_env.get_config().get_configuration()
    # conf.set_string("table.exec.mini-batch.enabled", "true")
    conf.set_string("table.exec.mini-batch.size", "500")
    conf.set_string("table.exec.mini-batch.allowed-latency", "100ms")
    conf.set_string("state.backend", "rocksdb")
    conf.set_string("table.exec.mini-batch.enabled", "false")


    # ---------------------------------------------------------
    #  1. Define Kafka Source Table (from Confluent Kafka)
    # ---------------------------------------------------------
    t_env.execute_sql(f"""
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
            event_time AS `timestamp`,
            WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
            'properties.group.id' = '{KAFKA_GROUP_ID}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # ---------------------------------------------------------
    #  2. Define Hierarchical Aggregations
    # ---------------------------------------------------------
    # Product-level aggregates
    product_agg = t_env.sql_query("""
        SELECT
            category,
            name AS product_name,
            productId,
            status,
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            SUM(quantity) AS total_qty,
            SUM(weightKg) AS total_weight,
            AVG(weightKg) AS avg_weight,
            COUNT(*) AS event_count
        FROM product_events
        GROUP BY category, name, productId, status,
                 TUMBLE(event_time, INTERVAL '1' MINUTE)
    """)

    # Subcategory (category + product_name)
    subcat_agg = t_env.sql_query("""
        SELECT
            category,
            name AS product_name,
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            SUM(quantity) AS total_qty,
            SUM(weightKg) AS total_weight,
            COUNT(*) AS event_count
        FROM product_events
        GROUP BY category, name, TUMBLE(event_time, INTERVAL '1' MINUTE)
    """)

    # Category-level
    category_agg = t_env.sql_query("""
        SELECT
            category,
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            SUM(quantity) AS total_qty,
            SUM(weightKg) AS total_weight,
            COUNT(*) AS event_count
        FROM product_events
        GROUP BY category, TUMBLE(event_time, INTERVAL '1' MINUTE)
    """)

    # ---------------------------------------------------------
    #  3. Output Incremental Results (Console Sink)
    # ---------------------------------------------------------
    t_env.to_changelog_stream(product_agg).map(lambda r: f"[PRODUCT] {r}").print()
    t_env.to_changelog_stream(subcat_agg).map(lambda r: f"[SUBCATEGORY] {r}").print()
    t_env.to_changelog_stream(category_agg).map(lambda r: f"[CATEGORY] {r}").print()

    env.execute("Flink Product Hierarchical Aggregation from Kafka")


if __name__ == "__main__":
    main()
