# from pyflink.table import EnvironmentSettings, TableEnvironment

# def main():
#     # -------------------- Flink Environment --------------------
#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = TableEnvironment.create(settings)

#     # -------------------- Kafka Source --------------------
#     t_env.execute_sql("""
#         CREATE TABLE kafka_events (
#             `topic` STRING METADATA FROM 'topic' VIRTUAL,
#             `msg_value` STRING,
#             `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
#             WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
#         ) WITH (
#             'connector' = 'kafka',
#             'topic-pattern' = 'salestransactions|inventoryupdates|returns',
#             'properties.bootstrap.servers' = 'broker:29094',
#             'properties.group.id' = 'flink-consumer',
#             'scan.startup.mode' = 'earliest-offset',
#             'format' = 'json',
#             'json.ignore-parse-errors' = 'true'
#         )
#     """)

#     # -------------------- TimescaleDB Sink --------------------
#     t_env.execute_sql("""
#         CREATE TABLE inventory_summary (
#             product_id STRING,
#             total_sold BIGINT,
#             total_returned BIGINT,
#             net_inventory BIGINT,
#             last_update TIMESTAMP(3),
#             PRIMARY KEY (product_id) NOT ENFORCED
#         ) WITH (
#             'connector' = 'jdbc',
#             'url' = 'jdbc:postgresql://host.docker.internal:5434/maindb',
#             'table-name' = 'inventory_summary',
#             'driver' = 'org.postgresql.Driver',
#             'username' = 'admin',
#             'password' = 'admin123'
#         )
#     """)

#     # -------------------- Transform & Aggregate --------------------
#     # parse JSON payload differently based on topic
#     t_env.execute_sql("""
#         CREATE TEMPORARY VIEW parsed AS
#         SELECT
#             JSON_VALUE(msg_value, '$.product_id') AS product_id,
#             CASE
#                 WHEN topic = 'salestransactions' THEN CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 ELSE 0
#             END AS sold,
#             CASE
#                 WHEN topic = 'returns' THEN CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 ELSE 0
#             END AS returned,
#             CASE
#                 WHEN topic = 'inventoryupdates'
#                      AND JSON_VALUE(msg_value, '$.event_type') = 'shipment_received'
#                      THEN CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 WHEN topic = 'inventoryupdates'
#                      AND JSON_VALUE(msg_value, '$.event_type') = 'shipment_sent'
#                      THEN -CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 ELSE 0
#             END AS shipment_delta,
#             ts
#         FROM kafka_events
#     """)

#     # aggregate across events
#     t_env.execute_sql("""
#         INSERT INTO inventory_summary
#         SELECT
#             product_id,
#             SUM(sold) AS total_sold,
#             SUM(returned) AS total_returned,
#             SUM(-shipment_delta) AS shipments_sent,  -- optional breakdown
# #             SUM(sold) - SUM(returned) + SUM(shipment_delta) AS net_inventory,
# #             MAX(ts) AS last_update
# #         FROM parsed
# #         GROUP BY product_id
# #     """)

# # if __name__ == "__main__":
# #     main()

# from pyflink.table import EnvironmentSettings, TableEnvironment

# def main():
#     # Set up the streaming environment
#     env_settings = EnvironmentSettings.in_streaming_mode()
#     t_env = TableEnvironment.create(env_settings)

#     # Kafka Source (example — adjust topic and bootstrap server)
#     t_env.execute_sql("""
#         CREATE TABLE equipment_events (
#             product_id STRING,
#             quantity INT,
#             event_type STRING,
#             event_time TIMESTAMP_LTZ(3),
#             WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
#         ) WITH (
#             'connector' = 'kafka',
#             'topic' = 'equipment',
#             'properties.bootstrap.servers' = 'broker:29092',
#             'properties.group.id' = 'flink-group',
#             'format' = 'json',
#             'scan.startup.mode' = 'earliest-offset'
#         )
#     """)

#     # JDBC Sink to TimescaleDB (running on port 5434 inside Docker)
#     # JDBC Sink to TimescaleDB with PRIMARY KEY
#     t_env.execute_sql("""
#         CREATE TABLE inventory_summary (
#             product_id STRING,
#             total_sold BIGINT,
#             total_returned BIGINT,
#             net_inventory BIGINT,
#             last_update TIMESTAMP(3),
#             PRIMARY KEY (product_id) NOT ENFORCED
#         ) WITH (
#             'connector' = 'jdbc',
#             'url' = 'jdbc:postgresql://host.docker.internal:5434/maindb',
#             'table-name' = 'inventory_summary',
#             'username' = 'admin',
#             'password' = 'admin123',
#             'driver' = 'org.postgresql.Driver'
#         )
#     """)


#     # Aggregation query (no shipments_sent, fix last_update type)
#     t_env.execute_sql("""
#         INSERT INTO inventory_summary
#         SELECT
#             product_id,
#             SUM(CASE WHEN event_type = 'sold' THEN quantity ELSE 0 END) AS total_sold,
#             SUM(CASE WHEN event_type = 'returned' THEN quantity ELSE 0 END) AS total_returned,
#             SUM(CASE WHEN event_type = 'received' THEN quantity ELSE 0 END)
#               - SUM(CASE WHEN event_type = 'sold' THEN quantity ELSE 0 END) AS net_inventory,
#             CAST(MAX(event_time) AS TIMESTAMP(3)) AS last_update
#         FROM equipment_events
#         GROUP BY product_id
#     """)

# if __name__ == "__main__":
#     main()

# from pyflink.table import EnvironmentSettings, TableEnvironment

# def main():
#     # -------------------- Flink Environment --------------------
#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = TableEnvironment.create(settings)

#     # -------------------- Kafka Source --------------------
#     t_env.execute_sql("""
#         CREATE TABLE kafka_events (
#             `topic` STRING METADATA FROM 'topic' VIRTUAL,
#             `msg_value` STRING,
#             ts AS TO_TIMESTAMP_LTZ(JSON_VALUE(msg_value, '$.timestamp')),
#             WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
#         ) WITH (
#             'connector' = 'kafka',
#             'topic-pattern' = 'salestransactions|inventoryupdates|returns',
#             'properties.bootstrap.servers' = 'broker:29094',
#             'properties.group.id' = 'flink-consumer',
#             'scan.startup.mode' = 'earliest-offset',
#             'format' = 'json',
#             'json.ignore-parse-errors' = 'true'
#         )
#     """)

#     # -------------------- TimescaleDB Sink --------------------
#     t_env.execute_sql("""
#         CREATE TABLE inventory_summary (
#             product_id STRING,
#             total_sold BIGINT,
#             total_returned BIGINT,
#             net_inventory BIGINT,
#             last_update TIMESTAMP(3),
#             PRIMARY KEY (product_id) NOT ENFORCED
#         ) WITH (
#             'connector' = 'jdbc',
#             'url' = 'jdbc:postgresql://host.docker.internal:5434/maindb',
#             'table-name' = 'inventory_summary',
#             'driver' = 'org.postgresql.Driver',
#             'username' = 'admin',
#             'password' = 'admin123'
#         )
#     """)

#     # -------------------- Transform & Aggregate --------------------
#     t_env.execute_sql("""
#         CREATE TEMPORARY VIEW parsed AS
#         SELECT
#             JSON_VALUE(msg_value, '$.product_id') AS product_id,
#             CASE
#                 WHEN topic = 'salestransactions'
#                      THEN CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 ELSE 0
#             END AS sold,
#             CASE
#                 WHEN topic = 'returns'
#                      THEN CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 ELSE 0
#             END AS returned,
#             CASE
#                 WHEN topic = 'inventoryupdates'
#                      AND JSON_VALUE(msg_value, '$.event_type') = 'shipment_received'
#                      THEN CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 WHEN topic = 'inventoryupdates'
#                      AND JSON_VALUE(msg_value, '$.event_type') = 'shipment_sent'
#                      THEN -CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 ELSE 0
#             END AS shipment_delta,
#             ts
#         FROM kafka_events
#     """)

#     # -------------------- Aggregate & Insert --------------------
#     t_env.execute_sql("""
#         INSERT INTO inventory_summary
#         SELECT
#             product_id,
#             SUM(sold) AS total_sold,
#             SUM(returned) AS total_returned,
#             SUM(sold) - SUM(returned) + SUM(shipment_delta) AS net_inventory,
#             CAST(MAX(ts) AS TIMESTAMP(3)) AS last_update
#         FROM parsed
#         GROUP BY product_id
#     """)

# if __name__ == "__main__":
#     main()

# from pyflink.table import EnvironmentSettings, TableEnvironment

# def main():
#     # -------------------- Flink Environment --------------------
#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = TableEnvironment.create(settings)

#     # -------------------- Kafka Source --------------------
#     t_env.execute_sql("""
#         CREATE TABLE kafka_events (
#             `topic` STRING METADATA FROM 'topic' VIRTUAL,
#             `msg_value` STRING,
#             `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
#             WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
#         ) WITH (
#             'connector' = 'kafka',
#             'topic-pattern' = 'salestransactions|inventoryupdates|returns',
#             'properties.bootstrap.servers' = 'broker:29094',
#             'properties.group.id' = 'flink-consumer-debug',
#             'scan.startup.mode' = 'earliest-offset',
#             'format' = 'json',
#             'json.ignore-parse-errors' = 'true'
#         )
#     """)

#     # -------------------- Print Sink (for debugging) --------------------
#     t_env.execute_sql("""
#         CREATE TABLE print_sink (
#             product_id STRING,
#             sold BIGINT,
#             returned BIGINT,
#             shipment_delta BIGINT,
#             ts TIMESTAMP_LTZ(3)
#         ) WITH (
#             'connector' = 'print'
#         )
#     """)

#     # -------------------- Transform & Print --------------------
#     t_env.execute_sql("""
#         INSERT INTO print_sink
#         SELECT
#             JSON_VALUE(msg_value, '$.product_id') AS product_id,
#             CASE
#                 WHEN topic = 'salestransactions'
#                 THEN CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 ELSE 0
#             END AS sold,
#             CASE
#                 WHEN topic = 'returns'
#                 THEN CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 ELSE 0
#             END AS returned,
#             CASE
#                 WHEN topic = 'inventoryupdates'
#                      AND JSON_VALUE(msg_value, '$.event_type') = 'shipment_received'
#                 THEN CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 WHEN topic = 'inventoryupdates'
#                      AND JSON_VALUE(msg_value, '$.event_type') = 'shipment_sent'
#                 THEN -CAST(JSON_VALUE(msg_value, '$.quantity') AS BIGINT)
#                 ELSE 0
#             END AS shipment_delta,
#             TO_TIMESTAMP_LTZ(JSON_VALUE(msg_value, '$.timestamp'), 3) AS ts
#         FROM kafka_events
#     """)

# if __name__ == "__main__":
#     main()

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    # -------------------- Flink Environment --------------------
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # -------------------- Kafka Source --------------------
    t_env.execute_sql("""
        CREATE TABLE kafka_events (
            event_type   STRING,
            warehouse_id STRING,
            product_id   STRING,
            quantity     INT,
            event_time   TIMESTAMP_LTZ(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic-pattern' = 'salestransactions|inventoryupdates|returns',
            'properties.bootstrap.servers' = 'broker:29094',
            'properties.group.id' = 'flink-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.ignore-parse-errors' = 'true'
        );
    """)

    # -------------------- Debug Transformation --------------------
    table = t_env.sql_query("""
        SELECT event_type, warehouse_id, product_id, quantity, event_time
        FROM kafka_events
    """)

    # -------------------- Show Plan & Sample Data --------------------
    print("Execution Plan:")
    print(table.explain())

    print("\nStreaming Data (Ctrl+C to stop):")
    table.execute().print()


if __name__ == "__main__":
    main()
