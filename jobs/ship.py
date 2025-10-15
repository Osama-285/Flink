# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import (
#     StreamTableEnvironment, EnvironmentSettings
# )
# from pyflink.table.window import Tumble
# from pyflink.table.expressions import col, lit

# # 1️⃣ Setup environment
# env = StreamExecutionEnvironment.get_execution_environment()
# env.set_parallelism(1)

# settings = EnvironmentSettings.in_streaming_mode()
# t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# # 2️⃣ Define Kafka source (replace bootstrap server / topic as needed)
# t_env.execute_sql("""
# CREATE TABLE ship_events (
#     shipId STRING,
#     imoNumber STRING,
#     name STRING,
#     flag STRING,
#     capacityTEU INT,
#     totalContainers INT,
#     eventType STRING,
#     berthId STRING,
#     arrivalTime TIMESTAMP_LTZ(3),
#     departureTime TIMESTAMP_LTZ(3),
#     status STRING,
#     `timestamp` TIMESTAMP_LTZ(3),
#     WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '10' SECOND
# ) WITH (
#     'connector' = 'kafka',
#     'topic' = 'shipEvent',
#     'properties.bootstrap.servers' = 'broker:29094',
#     'properties.group.id' = 'flink_consumer',
#     'scan.startup.mode' = 'latest-offset',
#     'json.timestamp-format.standard' = 'ISO-8601',
#     'format' = 'json'
# )
# """)

# # 3️⃣ Print sink
# t_env.execute_sql("""
# CREATE TABLE ship_metrics (
#     window_start TIMESTAMP(3),
#     window_end TIMESTAMP(3),
#     total_ships BIGINT,
#     total_containers BIGINT,
#     avg_delay_hours DOUBLE,
#     delay_count BIGINT
# ) WITH (
#     'connector' = 'print'
# )
# """)

# # 4️⃣ Query: Tumbling 2-min window with multiple aggregations
# t_env.execute_sql("""
# INSERT INTO ship_metrics
# SELECT
#     window_start,
#     window_end,
#     COUNT(DISTINCT shipId) AS total_ships,
#     SUM(totalContainers) AS total_containers,
#     AVG(TIMESTAMPDIFF(HOUR, arrivalTime, departureTime)) AS avg_delay_hours,
#     SUM(CASE WHEN eventType = 'DELAY' THEN 1 ELSE 0 END) AS delay_count
# FROM TABLE(
#     TUMBLE(TABLE ship_events, DESCRIPTOR(`timestamp`), INTERVAL '2' MINUTE)
# )
# GROUP BY window_start, window_end
# """)

# # 5️⃣ Execute
# t_env.execute("ShipMetricsJob")
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings
)

def main():
    # 1️⃣ Setup environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.get_config().get_configuration().set_string("execution.runtime-mode", "streaming")

    # 2️⃣ Define Kafka source
    t_env.execute_sql("""
    CREATE TABLE ship_events (
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
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '10' SECOND
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

    # 3️⃣ Define print sink
    t_env.execute_sql("""
    CREATE TABLE ship_metrics (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        total_ships BIGINT,
        total_containers BIGINT,
        avg_delay_hours DOUBLE,
        delay_count BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """)

    # 4️⃣ Tumbling window aggregation (2 minutes)
    t_env.execute_sql("""
    INSERT INTO ship_metrics
    SELECT
        window_start,
        window_end,
        COUNT(DISTINCT shipId) AS total_ships,
        SUM(totalContainers) AS total_containers,
        AVG(TIMESTAMPDIFF(HOUR, arrivalTime, departureTime)) AS avg_delay_hours,
        SUM(CASE WHEN eventType = 'DELAY' THEN 1 ELSE 0 END) AS delay_count
    FROM TABLE(
        TUMBLE(TABLE ship_events, DESCRIPTOR(`timestamp`), INTERVAL '2' MINUTE)
    )
    GROUP BY window_start, window_end
    """)

    # 5️⃣ Execute job
    env.execute("ShipMetricsJob").print()


if __name__ == "__main__":
    main()
