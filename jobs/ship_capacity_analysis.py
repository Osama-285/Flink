# from pyflink.table import EnvironmentSettings, TableEnvironment


# def main():
#     # -------------------- Flink Environment --------------------
#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = TableEnvironment.create(settings)

#     # Increase parallelism
#     t_env.get_config().get_configuration().set_string("parallelism.default", "3")
#     t_env.execute_sql("""
#         CREATE TABLE ship_arrival (
#             shipId STRING,
#             imoNumber STRING,
#             name STRING,
#             flag STRING,
#             capacityTEU INT,
#             totalContainers INT,
#             eventType STRING,
#             berthId STRING,
#             arrivalTime TIMESTAMP_LTZ(3),
#             departureTime TIMESTAMP_LTZ(3),
#             status STRING,
#             `timestamp` TIMESTAMP_LTZ(3),
#             WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
#         ) WITH (
#             'connector' = 'kafka',
#             'topic' = 'shipEvent',
#             'properties.bootstrap.servers' = 'broker:29094',
#             'properties.group.id' = 'flink_consumer',
#             'scan.startup.mode' = 'earliest-offset',
#             'json.timestamp-format.standard' = 'ISO-8601',
#             'format' = 'json'
#         )
#     """)

#     t_env.execute_sql("""
#         CREATE TABLE capacity_status_print (
#             shipId STRING,
#             name STRING,
#             capacityTEU INT,
#             totalContainers INT,
#             capacity_utilization_percent DOUBLE,
#             capacity_status STRING
#         ) WITH (
#             'connector' = 'print'
#         )
#     """)

#     # -------------------- Transformation / Query --------------------
#     query = """
#         INSERT INTO capacity_status_print
#         SELECT
#             shipId,
#             name,
#             capacityTEU,
#             totalContainers,
#             ROUND((totalContainers * 100.0 / capacityTEU), 2) AS capacity_utilization_percent,
#             CASE
#                 WHEN totalContainers > 250 THEN 'Overloaded'
#                 WHEN totalContainers < 100 THEN 'Underloaded'
#                 ELSE 'Normal'
#             END AS capacity_status
#         FROM ship_arrival
#         WHERE eventType = 'ARRIVAL'
#     """

#     # -------------------- Execute the Insert Query --------------------
#     t_env.execute_sql(query)

# if __name__ == "__main__":
#     main()

from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # -------------------- Flink Environment --------------------
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # Optional: Set parallelism
    t_env.get_config().get_configuration().set_string("parallelism.default", "3")

    # -------------------- Source Table (Kafka) --------------------
    t_env.execute_sql("""
        CREATE TABLE ship_arrival (
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
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'shipEvent',
            'properties.bootstrap.servers' = 'broker:29094',
            'properties.group.id' = 'flink_capacity_windowed_consumer',
            'scan.startup.mode' = 'earliest-offset',
            'json.timestamp-format.standard' = 'ISO-8601',
            'format' = 'json'
        )
    """)

    # -------------------- Sink Table (Print) --------------------
    t_env.execute_sql("""
        CREATE TABLE capacity_windowed_print (
            shipId STRING,
            name STRING,
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            total_arrivals BIGINT,
            avg_containers DOUBLE,
            avg_utilization_percent DOUBLE,
            capacity_status STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    # -------------------- Tumbling Window Aggregation --------------------
    query = """
        INSERT INTO capacity_windowed_print
        SELECT
            shipId,
            name,
            window_start,
            window_end,
            COUNT(*) AS total_arrivals,
            AVG(totalContainers) AS avg_containers,
            AVG(ROUND((totalContainers * 100.0 / capacityTEU), 2)) AS avg_utilization_percent,
            CASE
                WHEN AVG(totalContainers) > 250 THEN 'Overloaded'
                WHEN AVG(totalContainers) < 100 THEN 'Underloaded'
                ELSE 'Normal'
            END AS capacity_status
        FROM TABLE(
            TUMBLE(TABLE ship_arrival, DESCRIPTOR(`timestamp`), INTERVAL '5' MINUTES)
        )
        WHERE eventType = 'ARRIVAL'
        GROUP BY shipId, name, window_start, window_end
    """

    # -------------------- Execute the Query --------------------
    t_env.execute_sql(query)

if __name__ == "__main__":
    main()
