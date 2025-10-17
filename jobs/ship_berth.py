# from pyflink.table import EnvironmentSettings, TableEnvironment

# def main():
#     # -------------------- Flink Environment --------------------
#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = TableEnvironment.create(settings)

#     # Set parallelism to 3
#     t_env.get_config().get_configuration().set_string("parallelism.default", "3")

#     # -------------------- Source Table (ship_arrival) --------------------
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

#     # -------------------- Sink Table (PostgreSQL) --------------------
#     t_env.execute_sql("""
#         CREATE TABLE berth_yearly_stats (
#             berthId STRING,
#             eventType STRING,
#             arrival_year BIGINT,
#             total_arrivals INT,
#             total_departures INT,
#             avg_turnaround_minutes INT,
#             PRIMARY KEY (berthId, arrival_year) NOT ENFORCED
#         ) WITH (
#             'connector' = 'print'
#         )
#     """)

#     # -------------------- Aggregation Query --------------------
#     query = """
#         INSERT INTO berth_yearly_stats
#         SELECT
#             berthId,
#             eventType,
#             EXTRACT(YEAR FROM arrivalTime) AS arrival_year,
#             SUM(CASE WHEN eventType = 'ARRIVAL' THEN 1 ELSE 0 END) AS total_arrivals,
#             SUM(CASE WHEN eventType = 'DEPARTURE' THEN 1 ELSE 0 END) AS total_departures,
#             AVG(TIMESTAMPDIFF(MINUTE, arrivalTime, departureTime)) AS avg_turnaround_minutes
#         FROM ship_arrival
#         WHERE eventType IN ('ARRIVAL', 'DEPARTURE')
#         GROUP BY berthId, eventType,EXTRACT(YEAR FROM arrivalTime)
#     """

#     # -------------------- Execute --------------------
#     t_env.execute_sql(query)

# if __name__ == "__main__":
#     main()


from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # -------------------- Flink Environment --------------------
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # Increase parallelism
    t_env.get_config().get_configuration().set_string("parallelism.default", "3")

    # -------------------- Kafka Source --------------------
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
            'properties.group.id' = 'flink_consumer',
            'scan.startup.mode' = 'earliest-offset',
            'json.timestamp-format.standard' = 'ISO-8601',
            'format' = 'json'
        )
    """)

    # -------------------- PostgreSQL Sink --------------------
    t_env.execute_sql("""
        CREATE TABLE berth_window_stats (
            berthId STRING,
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            total_arrivals INT,
            total_departures INT,
            avg_turnaround_minutes DOUBLE,
            PRIMARY KEY (berthId, window_start, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'print'
        )
    """)

    # -------------------- Tumbling Window Aggregation Query --------------------
    query = """
        INSERT INTO berth_window_stats
        SELECT
            berthId,
            window_start,
            window_end,
            SUM(CASE WHEN eventType = 'ARRIVAL' THEN 1 ELSE 0 END) AS total_arrivals,
            SUM(CASE WHEN eventType = 'DEPARTURE' THEN 1 ELSE 0 END) AS total_departures,
            AVG(TIMESTAMPDIFF(MINUTE, arrivalTime, departureTime)) AS avg_turnaround_minutes
        FROM TABLE(
            TUMBLE(TABLE ship_arrival, DESCRIPTOR(`timestamp`), INTERVAL '2' MINUTES)
        )
        WHERE eventType IN ('ARRIVAL', 'DEPARTURE')
        GROUP BY berthId, window_start, window_end
    """

    # -------------------- Execute --------------------
    t_env.execute_sql(query)

if __name__ == "__main__":
    main()
