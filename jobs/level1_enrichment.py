# from pyflink.table import EnvironmentSettings, TableEnvironment

# def main():
#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env= TableEnvironment.create(settings)
#     t_env.get_config().get_configuration().set_string("parallelism.default", "4")


#     t_env.execute_sql("""
#     CREATE TABLE container_events (
#         containerId STRING,
#         shipId STRING,
#         size STRING,
#         weightKg INT,
#         sealNumber STRING,
#         eventType STRING,
#         location STRING,
#         status STRING,
#         `timestamp` TIMESTAMP_LTZ(3),
#         WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '60' SECOND
#     ) WITH (
#         'connector' = 'kafka',
#         'topic' = 'containerEvent',
#         'properties.bootstrap.servers' = 'broker:29094',
#         'properties.group.id' = 'flink_consumer',
#         'scan.startup.mode' = 'earliest-offset',
#         'value.format' = 'json',
#         'value.json.timestamp-format.standard' = 'ISO-8601',
#         'value.json.ignore-parse-errors' = 'true'
#     )
#     """)

#     t_env.execute_sql("""
#     CREATE TABLE ship_event (
#         shipId STRING,
#         imoNumber STRING,
#         name STRING,
#         flag STRING,
#         capacityTEU INT,
#         totalContainers INT,
#         eventType STRING,
#         berthId STRING,
#         arrivalTime TIMESTAMP_LTZ(3),
#         departureTime TIMESTAMP_LTZ(3),
#         status STRING,
#         `timestamp` TIMESTAMP_LTZ(3),
#         WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '60' SECOND,
#         PRIMARY KEY (shipId) NOT ENFORCED

#     ) WITH (
#         'connector' = 'upsert-kafka',
#         'topic' = 'shipEvent',
#         'properties.bootstrap.servers' = 'broker:29094',
#         'key.format' = 'json',
#         'value.format' = 'json',
#         'value.json.ignore-parse-errors' = 'true'
#     )
#     """)
#     # t_env.execute_sql("DESCRIBE ship_event").print()
#     # t_env.execute_sql("DESCRIBE contaner_events").print()
#     result = t_env.sql_query("""
#     SELECT
#         c.containerId,
#         c.shipId,
#         c.size,
#         c.weightKg,
#         s.capacityTEU,
#         (CAST(c.weightKg AS DOUBLE) / (s.capacityTEU * 1000.0)) AS utilization_ratio,
#         c.location,
#         c.status,
#         c.`timestamp`
#         FROM container_events AS c
#         LEFT JOIN ship_event FOR SYSTEM_TIME AS OF c.`timestamp` AS s
#         ON c.shipId = s.shipId
#     """)
#     t_env.execute_sql("""
#     CREATE TABLE enriched_sink (
#         containerId STRING,
#         shipId STRING,
#         size STRING,
#         weightKg INT,
#         capacityTEU INT,
#         utilization_ratio DOUBLE,
#         location STRING,
#         status STRING,
#         ts TIMESTAMP(3)
#         ) WITH (
#         'connector' = 'print'
#         )
#     """)
#     # result.print_schema()
#     t_env.execute_sql("DESCRIBE ship_event").print()
#     t_env.execute_sql("SHOW CREATE TABLE ship_event").print()
#     result.execute_insert("enriched_sink").print()


# if __name__ == "__main__":
#     main()

# from pyflink.table import EnvironmentSettings, TableEnvironment

# def main():
#     # -------------------- Environment --------------------
#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = TableEnvironment.create(settings)

#     # -------------------- Source 1: Container Events --------------------
#     t_env.execute_sql("""
#     CREATE TABLE container_events (
#         containerId STRING,
#         shipId STRING,
#         size STRING,
#         weightKg DOUBLE,
#         sealNumber STRING,
#         eventType STRING,
#         location STRING,
#         status STRING,
#         kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
#         WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '60' SECOND
#     ) WITH (
#         'connector' = 'kafka',
#         'topic' = 'containerEvent',
#         'properties.bootstrap.servers' = 'broker:29094',
#         'properties.group.id' = 'flink-container-enrich',
#         'format' = 'json',
#         'json.timestamp-format.standard' = 'ISO-8601',
#         'scan.startup.mode' = 'earliest-offset'
#     )
#     """)

#     # -------------------- Source 2: Ship Events --------------------
#     t_env.execute_sql("""
#     CREATE TABLE ship_events (
#         shipId STRING,
#         imoNumber STRING,
#         name STRING,
#         flag STRING,
#         capacityTEU INT,
#         totalContainers INT,
#         eventType STRING,
#         berthId STRING,
#         arrivalTime TIMESTAMP_LTZ(6),
#         departureTime TIMESTAMP_LTZ(6),
#         status STRING,
#         kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM `timestamp`,
#         WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '60' SECOND,
#         PRIMARY KEY (shipId) NOT ENFORCED
#     ) WITH (
#         'connector' = 'upsert-kafka',
#         'topic' = 'shipEvent',
#         'properties.bootstrap.servers' = 'broker:29094',
#         'key.format' = 'json',
#         'value.format' = 'json',
#         'json.timestamp-format.standard' = 'ISO-8601',
#         'value.fields-include' = 'ALL'
#     )
#     """)

#     # -------------------- Convert Ship Stream to Versioned Table --------------------
#     # (Allows time-based join using FOR SYSTEM_TIME AS OF)
#     t_env.execute_sql("""
#     CREATE VIEW ship_meta AS
#     SELECT
#         shipId,
#         imoNumber,
#         name,
#         flag,
#         capacityTEU,
#         totalContainers,
#         status,
#         `timestamp` AS rowtime
#     FROM ship_events
#     """)

#     # -------------------- Sink: Print Enriched Results --------------------
#     t_env.execute_sql("""
#     CREATE TABLE print_sink (
#         containerId STRING,
#         shipId STRING,
#         size STRING,
#         weightKg DOUBLE,
#         capacityTEU DOUBLE,
#         utilization_ratio DOUBLE,
#         shipStatus STRING,
#         location STRING,
#         containerStatus STRING,
#         `timestamp` TIMESTAMP_LTZ(3)
#     ) WITH (
#         'connector' = 'print'
#     )
#     """)

#     # -------------------- Enrichment Query --------------------
#     # result = t_env.execute_sql("""
#     # INSERT INTO print_sink
#     # SELECT
#     #     c.containerId,
#     #     c.shipId,
#     #     c.size,
#     #     c.weightKg,
#     #     s.capacityTEU,
#     #     (c.weightKg / (s.capacityTEU * 1000.0)) AS utilization_ratio,
#     #     s.status AS shipStatus,
#     #     c.location,
#     #     c.status AS containerStatus,
#     #     c.`timestamp`
#     # FROM container_events AS c
#     # LEFT JOIN ship_meta FOR SYSTEM_TIME AS OF c.`timestamp` AS s
#     #   ON c.shipId = s.shipId
#     # """)
#     # result.print()
#     t_env.execute_sql("""
#     INSERT INTO print_sink
#     SELECT
#         c.containerId,
#         c.shipId,
#         c.size,
#         c.weightKg,
#         s.capacityTEU,
#         (c.weightKg / (s.capacityTEU * 1000.0)) AS utilization_ratio,
#         s.status AS shipStatus,
#         c.location,
#         c.status AS containerStatus,
#         c.`timestamp`
#     FROM container_events AS c
#     LEFT JOIN ship_meta FOR SYSTEM_TIME AS OF c.`timestamp` AS s
#     ON c.shipId = s.shipId
#     """).wait()
# 

from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # -------------------- Source 1: Container Events --------------------
    t_env.execute_sql("""
    CREATE TABLE container_events (
        containerId STRING,
        shipId STRING,
        size STRING,
        weightKg DOUBLE,
        sealNumber STRING,
        eventType STRING,
        location STRING,
        status STRING,
        `timestamp` TIMESTAMP_LTZ(3),
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '60' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'containerEvent',
        'properties.bootstrap.servers' = 'broker:29094',
        'properties.group.id' = 'flink-container-enrich',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'scan.startup.mode' = 'earliest-offset'
    )
    """)

    # -------------------- Source 2: Ship Events --------------------
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
        departureTime TIMESTAMP_LTZ(3) NULL,
        status STRING,
        `timestamp` TIMESTAMP_LTZ(3),
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '60' SECOND,
        PRIMARY KEY(shipId) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'shipEvent',
        'properties.bootstrap.servers' = 'broker:29094',
        'key.format' = 'raw',
        'value.format' = 'json',
        'value.fields-include' = 'ALL',
        'value.json.ignore-parse-errors' = 'true',
        'value.json.timestamp-format.standard' = 'ISO-8601' 
    )
    """)

    # -------------------- Convert Ship Stream to Versioned Table --------------------
    t_env.execute_sql("""
    CREATE VIEW ship_meta AS
    SELECT
        shipId,
        imoNumber,
        name,
        flag,
        capacityTEU,
        totalContainers,
        status,
        `timestamp`
    FROM ship_events
    """)

    # -------------------- Sink: Print Enriched Results --------------------
    t_env.execute_sql("""
    CREATE TABLE print_sink (
        containerId STRING,
        shipId STRING,
        size STRING,
        weightKg DOUBLE,
        capacityTEU DOUBLE,
        utilization_ratio DOUBLE,
        shipStatus STRING,
        location STRING,
        containerStatus STRING,
        `timestamp` TIMESTAMP_LTZ(3)
    ) WITH (
        'connector' = 'print'
    )
    """)

    # -------------------- Enrichment Query --------------------
    t_env.execute_sql("""
    INSERT INTO print_sink
    SELECT
        c.containerId,
        c.shipId,
        c.size,
        c.weightKg,
        s.capacityTEU,
        (c.weightKg / (s.capacityTEU * 1000.0)) AS utilization_ratio,
        s.status AS shipStatus,
        c.location,
        c.status AS containerStatus,
        c.`timestamp`
    FROM container_events AS c
    LEFT JOIN ship_meta FOR SYSTEM_TIME AS OF c.`timestamp` AS s
    ON c.shipId = s.shipId
    """).wait()


if __name__ == "__main__":
    main()

