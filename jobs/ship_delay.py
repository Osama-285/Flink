# # from pyflink.table import EnvironmentSettings, TableEnvironment

# # def main():
# #     # -------------------- Flink Environment --------------------
# #     settings = EnvironmentSettings.in_streaming_mode()
# #     t_env = TableEnvironment.create(settings)
    
# #     # -------------------- Source Table --------------------
# #     t_env.execute_sql("""
# #     CREATE TABLE ship_info (
# #         shipId STRING,
# #         imoNumber STRING,
# #         name STRING,
# #         flag STRING,
# #         capacityTEU INT,
# #         totalContainers INT,
# #         eventType STRING,
# #         berthId STRING,
# #         arrivalTime TIMESTAMP_LTZ(3),
# #         departureTime TIMESTAMP_LTZ(3),
# #         status STRING,
# #         `timestamp` TIMESTAMP_LTZ(3),
# #         WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '60' SECOND
# #     ) WITH (
# #         'connector' = 'kafka',
# #         'topic' = 'shipEvent',
# #         'properties.bootstrap.servers' = 'broker:29094',
# #         'properties.group.id' = 'consumerF',
# #         'scan.startup.mode' = 'latest-offset',
# #         'json.timestamp-format.standard' = 'ISO-8601',
# #         'format' = 'json'
# #     )
# #     """)

# #     # -------------------- Delay Analysis Query --------------------
# #     query = """
# #     WITH avg_dwell AS (
# #         SELECT
# #             AVG(TIMESTAMPDIFF(MINUTE, arrivalTime, departureTime)) AS expected_dwell_minutes
# #         FROM ship_info
# #         WHERE status = 'DEPARTED'
# #     )
# #     SELECT
# #         s.shipId,
# #         s.name,
# #         s.berthId,
# #         s.flag,
# #         TIMESTAMPDIFF(MINUTE, s.arrivalTime, s.departureTime) AS actual_dwell_minutes,
# #         a.expected_dwell_minutes,
# #         (TIMESTAMPDIFF(MINUTE, s.arrivalTime, s.departureTime) - a.expected_dwell_minutes) AS delay_minutes
# #     FROM ship_info AS s, avg_dwell AS a
# #     WHERE s.status = 'DEPARTED'
# #       AND s.departureTime IS NOT NULL
# #       AND TIMESTAMPDIFF(MINUTE, s.arrivalTime, s.departureTime) > a.expected_dwell_minutes
# #     """

# #     result_table = t_env.sql_query(query)

# #     # -------------------- Output Sink (Print or Kafka) --------------------
# #     # For now, print to console
# #     result_table.execute().print()

# # if __name__ == "__main__":
# #     main()


# from pyflink.table import EnvironmentSettings, TableEnvironment

# def main():
#     # -------------------- Flink Environment --------------------
#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = TableEnvironment.create(settings)
    
#     # -------------------- Source Table --------------------
#     t_env.execute_sql("""
#     CREATE TABLE ship_info (
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
#         WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '60' SECOND
#     ) WITH (
#         'connector' = 'kafka',
#         'topic' = 'shipEvent',
#         'properties.bootstrap.servers' = 'broker:29094',
#         'properties.group.id' = 'consumerF',
#         'scan.startup.mode' = 'earliest-offset',
#         'json.timestamp-format.standard' = 'ISO-8601',
#         'format' = 'json'
#     )
#     """)

#     # -------------------- Print Sink --------------------
#     t_env.execute_sql("""
#     CREATE TABLE ship_delay_output (
#         shipId STRING,
#         name STRING,
#         berthId STRING,
#         flag STRING,
#         actual_dwell_minutes BIGINT,
#         expected_dwell_minutes BIGINT,
#         delay_minutes BIGINT
#     ) WITH (
#         'connector' = 'print'
#     )
#     """)

#     # -------------------- Delay Analysis Query --------------------
#     t_env.execute_sql("""
#     INSERT INTO ship_delay_output
#     WITH avg_dwell AS (
#         SELECT
#             AVG(TIMESTAMPDIFF(MINUTE, arrivalTime, departureTime)) AS expected_dwell_minutes
#         FROM ship_info
#         WHERE status = 'DEPARTED'
#     )
#     SELECT
#         s.shipId,
#         s.name,
#         s.berthId,
#         s.flag,
#         TIMESTAMPDIFF(MINUTE, s.arrivalTime, s.departureTime) AS actual_dwell_minutes,
#         a.expected_dwell_minutes,
#         (TIMESTAMPDIFF(MINUTE, s.arrivalTime, s.departureTime) - a.expected_dwell_minutes) AS delay_minutes
#     FROM ship_info AS s, avg_dwell AS a
#     WHERE s.status = 'DEPARTED'
#       AND s.departureTime IS NOT NULL
#       AND TIMESTAMPDIFF(MINUTE, s.arrivalTime, s.departureTime) > a.expected_dwell_minutes
#     """)

# if __name__ == "__main__":
#     main()

# from pyflink.table import EnvironmentSettings, TableEnvironment

# def main():
#     # -------------------- Flink Environment --------------------
#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = TableEnvironment.create(settings)
    
#     # -------------------- Source Table --------------------
#     t_env.execute_sql("""
#     CREATE TABLE ship_info (
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
#         WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '60' SECOND
#     ) WITH (
#         'connector' = 'kafka',
#         'topic' = 'shipEvent',
#         'properties.bootstrap.servers' = 'broker:29094',
#         'properties.group.id' = 'flink_consumer',
#         'scan.startup.mode' = 'earliest-offset',
#         'json.timestamp-format.standard' = 'ISO-8601',
#         'format' = 'json'
#     )
#     """)

#     # -------------------- Delay Analysis Query --------------------
#     query = """
#     WITH avg_dwell AS (
#         SELECT
#             AVG(TIMESTAMPDIFF(MINUTE, arrivalTime, departureTime)) AS expected_dwell_minutes
#         FROM ship_info
#         WHERE status = 'DEPARTED'
#     )
#     SELECT
#         s.shipId,
#         s.name,
#         s.berthId,
#         s.flag,
#         TIMESTAMPDIFF(MINUTE, s.arrivalTime, s.departureTime) AS actual_dwell_minutes,
#         a.expected_dwell_minutes,
#         (TIMESTAMPDIFF(MINUTE, s.arrivalTime, s.departureTime) - a.expected_dwell_minutes) AS delay_minutes
#     FROM ship_info AS s, avg_dwell AS a
#     WHERE s.status = 'DEPARTED'
#       AND s.departureTime IS NOT NULL
#       AND TIMESTAMPDIFF(MINUTE, s.arrivalTime, s.departureTime) > a.expected_dwell_minutes
#     """

#     result_table = t_env.sql_query(query)
#     result_table.execute().print()

# if __name__ == "__main__":
#     main()

from pyflink.table import EnvironmentSettings, TableEnvironment,StreamTableEnvironment,expressions as expr
from pyflink.table.expressions import col, lit, call_sql
from pyflink.datastream import StreamExecutionEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(5)
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    # t_env.get_config().get_configuration().set_string("parallelism.default", "5")
    # t_env.get_config().set("table.exec.resource.default-parallelism", "5")


    t_env.execute_sql("""
    CREATE TABLE ship_info (
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
        'scan.startup.mode' = 'earliest-offset',
        'json.timestamp-format.standard' = 'ISO-8601',
        'format' = 'json'
    )
    """)
    ship_info = t_env.from_path("ship_info")
    avg_dwell=(
        ship_info.filter(expr.col("status") == expr.lit("DEPARTED"))
        .select(
            expr.call(
                "AVG", expr.call_sql("TIMESTAMPDIFF(MINUTE, departureTime,arrivalTime)")
            ).alias("expected_dwell_minutes")
        )
    )
    # result = (
    #     ship_info
    #     .filter(
    #         (expr.col("status") == expr.lit("DEPARTED")) &
    #         col("departureTime").is_not_null
    #     )
    #     .join(avg_dwell)  # Cartesian join since avg_dwell is single-row
    #     .select(
    #         expr.col("shipId"),
    #         expr.col("name"),
    #         expr.col("berthId"),
    #         expr.col("flag"),
    #         expr.call_sql("TIMESTAMPDIFF(MINUTE, arrivalTime, departureTime)").alias("actual_dwell_minutes"),
    #         expr.col("expected_dwell_minutes"),
    #         (
    #             expr.call_sql("TIMESTAMPDIFF(MINUTE, arrivalTime, departureTime)")
    #             - expr.col("expected_dwell_minutes")
    #         ).alias("delay_minutes")
    #     )
    #     .filter(
    #         expr.call_sql("TIMESTAMPDIFF(MINUTE, arrivalTime, departureTime)") > expr.col("expected_dwell_minutes")
    #     )
    # )

    # result = (
    #     ship_info.filter(
    #         (col("status") == expr.lit("DEPARTED")) &
    #         (col("departureTime").is_not_null) & (
    #             expr.call_sql("TIMESTAMPDIFF(MINUTE, departureTime,arrivalTime)") > expr.col("a.expected_dwell_minutes")
    #         )
    #     ).join(avg_dwell.alias("a"))
    #     .select(
    #         expr.col("shipId"),
    #         expr.col("name"),
    #         expr.col("berthId"),
    #         expr.col("flag"),
    #         expr.call_sql("TIMESTAMPDIFF(MINUTE, departureTime,arrivalTime)").alias("actual_dwell_minutes"),
    #         expr.col("a.expected_dwell_minutes"),
    #         (
    #             expr.call_sql("TIMESTAMPDIFF(MINUTE, departureTime,arrivalTime)") -  expr.col("a.expected_dwell_minutes")
    #         ).alias("delay_minutes")

    #     )
    # )
    # result = (
    #     ship_info
    #     .join(avg_dwell)  # join first
    #     .where(
    #         (col("status") == expr.lit("DEPARTED")) &
    #         (col("departureTime").is_not_null) &
    #         (
    #             expr.call_sql("TIMESTAMPDIFF(MINUTE, departureTime, arrivalTime)") >
    #             col("a.expected_dwell_minutes")
    #         )
    #     )
    #     .select(
    #         col("s.shipId"),
    #         col("s.name"),
    #         col("s.berthId"),
    #         col("s.flag"),
    #         expr.call_sql("TIMESTAMPDIFF(MINUTE, s.departureTime,s.arrivalTime)").alias("actual_dwell_minutes"),
    #         col("a.expected_dwell_minutes"),
    #         (
    #             expr.call_sql("TIMESTAMPDIFF(MINUTE,s.departureTime, s.arrivalTime)") -
    #             col("a.expected_dwell_minutes")
    #         ).alias("delay_minutes")
    #     )
    # )
    result = ship_info.join(avg_dwell)
    final_result = (
        result.filter(
            (col("status") == expr.lit("DEPARTED")) &
            (col("departureTime").is_not_null) &
            (
                expr.call_sql("TIMESTAMPDIFF(MINUTE, departureTime, arrivalTime)") > col("expected_dwell_minutes")
            )
        ).select(
            col("shipId"),
            col("name"),
            col("berthId"),
            col("flag"),
            expr.call_sql("TIMESTAMPDIFF(MINUTE, departureTime,arrivalTime)").alias("actual_dwell_minutes"),
            col("expected_dwell_minutes"),
            (
                expr.call_sql("TIMESTAMPDIFF(MINUTE,departureTime, arrivalTime)") - col("expected_dwell_minutes")
            ).alias("delay_minutes")
        )
    )

    # result.execute().print()
    # result.print_schema()
    final_result.execute().print()


if __name__ == "__main__":
    main()