# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import EnvironmentSettings, StreamTableEnvironment


# KAFKA_BROKERS = "broker:29094"
# KAFKA_TOPIC = "containerEvent"
# DELAY_THRESHOLD_SECONDS = 24 * 3600

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(2)
#     env.enable_checkpointing(30000)

#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = StreamTableEnvironment.create(env, environment_settings = settings)

#     t_env.execute_sql(f"""
#     CREATE TABLE container_events (
#         containerId STRING,
#         shipId STRING,
#         size STRING,
#         weightKg INT,
#         sealNumber STRING,
#         eventType STRING,
#         location STRING,
#         status STRING,
#         `timestamp` STRING,
#         event_time AS TO_TIMESTAMP(`timestamp`),
#         WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
#     ) WITH (
#         'connector' = 'kafka',
#         'topic' = '{KAFKA_TOPIC}',
#         'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
#         'properties.group.id' = 'container-analytics-group',
#         'scan.startup.mode' = 'latest-offset',
#         'format' = 'json',
#         'json.ignore-parse-errors' = 'true'
#         )              
#     """)

#     t_env.execute_sql("""
#     CREATE TABLE print_sink (
#         label STRING,
#         yard STRING,
#         metric_value DOUBLE,
#         window_start TIMESTAMP_LTZ(3),
#         window_end TIMESTAMP_LTZ(3)
#     ) WITH (
#         'connector' = 'print'
#     )
#     """)

#     t_env.execute_sql("""
#     CREATE TEMPORARY VIEW container_dwell AS
#     SELECT
#         u.containerId,
#         u.location AS yard,
#         (CAST(d.event_time AS BIGINT) - CAST(u.event_time AS BIGINT)) / 1000 AS dwell_seconds,
#         u.event_time
#     FROM container_events AS u
#     JOIN container_events AS d
#         ON u.containerId = d.containerId
#     WHERE u.eventType = 'UNLOAD'
#         AND d.status = 'DELIVERED'
#         AND d.event_time > u.event_time
#     """)

#     t_env.execute_sql("""
#     INSERT INTO print_sink
#     SELECT
#         'AVG_DWELL' AS label,
#         yard,
#         AVG(dwell_seconds) AS metric_value,
#         window_start,
#         window_end
#     FROM TABLE(
#         TUMBLE(TABLE container_dwell, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
#     )
#     GROUP BY yard, window_start, window_end
#     """)

#     # ---------------------------
#     # Metric 2: Turnaround time (LOAD → DELIVERED)
#     # ---------------------------
#     t_env.execute_sql("""
#     CREATE TEMPORARY VIEW container_turnaround AS
#     SELECT
#         l.containerId,
#         l.location AS yard,
#         (CAST(d.event_time AS BIGINT) - CAST(u.event_time AS BIGINT)) / 1000 AS turnaround_seconds,
#         l.event_time
#     FROM container_events AS l
#     JOIN container_events AS d
#         ON l.containerId = d.containerId
#     WHERE l.eventType = 'LOAD'
#         AND d.status = 'DELIVERED'
#         AND d.event_time > l.event_time
#     """)

#     t_env.execute_sql("""
#     INSERT INTO print_sink
#     SELECT
#         'TURNAROUND' AS label,
#         yard,
#         AVG(turnaround_seconds) AS metric_value,
#         window_start,
#         window_end
#     FROM TABLE(
#         TUMBLE(TABLE container_turnaround, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
#     )
#     GROUP BY yard, window_start, window_end
#     """)


#     # ---------------------------
#     # Metric 3: % delayed containers (> 24h dwell)
#     # ---------------------------
#     t_env.execute_sql(f"""
#     CREATE TEMPORARY VIEW container_delay_flag AS
#     SELECT
#         containerId,
#         yard,
#         CASE WHEN dwell_seconds > {DELAY_THRESHOLD_SECONDS * 1000} THEN 1 ELSE 0 END AS is_delayed,
#         event_time
#     FROM container_dwell
#     """)

#     t_env.execute_sql("""
#     INSERT INTO print_sink
#     SELECT
#         'DELAY_PERCENT' AS label,
#         yard,
#         (CAST(SUM(is_delayed) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)) * 100.0 AS metric_value,
#         window_start,
#         window_end
#     FROM TABLE(
#         TUMBLE(TABLE container_delay_flag, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
#     )
#     GROUP BY yard, window_start, window_end
#     """)
#     # ---------------------------
#     # Execute Job
#     # ---------------------------
#     print("Submitting Flink job to print real-time container metrics...")
#     # t_env.print("container_analytics_print")
#     # t_env.execute_sql("DESCRIBE container_events").print()
#     # t_env.execute_sql("DESCRIBE container_dwell").print()
#     # t_env.execute_sql("DESCRIBE container_turnaround").print()
#     # t_env.execute_sql("DESCRIBE container_delay_flag").print()
#     t_env.execute_sql("DESCRIBE container_dwell").print()
#     t_env.from_path("container_dwell").execute().print()

    


# if __name__ == "__main__":
#     main()

# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# KAFKA_BROKERS = "broker:29094"
# KAFKA_TOPIC = "containerEvent"
# DELAY_THRESHOLD_SECONDS = 24 * 3600

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(2)
#     env.enable_checkpointing(30000)

#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = StreamTableEnvironment.create(env, environment_settings=settings)

#     # SOURCE: parse timestamp to a proper event-time with watermark
#     t_env.execute_sql(f"""
#     CREATE TABLE container_events (
#         containerId STRING,
#         shipId STRING,
#         size STRING,
#         weightKg INT,
#         sealNumber STRING,
#         eventType STRING,
#         location STRING,
#         status STRING,
#         `timestamp` STRING,
#         -- Use TO_TIMESTAMP_LTZ for robust ISO-8601 parsing with timezone awareness
#         event_time AS TO_TIMESTAMP_LTZ(`timestamp`),
#         WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
#     ) WITH (
#         'connector' = 'kafka',
#         'topic' = '{KAFKA_TOPIC}',
#         'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
#         'properties.group.id' = 'container-analytics-group',
#         'scan.startup.mode' = 'latest-offset',
#         'format' = 'json',
#         'json.ignore-parse-errors' = 'true'
#     )
#     """)

#     # debug print sink
#     t_env.execute_sql("""
#     CREATE TABLE print_sink (
#         label STRING,
#         yard STRING,
#         metric_value DOUBLE,
#         window_start TIMESTAMP_LTZ(3),
#         window_end TIMESTAMP_LTZ(3)
#     ) WITH (
#         'connector' = 'print'
#     )
#     """)

#     # IMPORTANT: keep event_time column name in the view (do not rename it)
#     t_env.execute_sql("""
#     CREATE TEMPORARY VIEW container_dwell AS
#     SELECT
#         u.containerId,
#         u.location AS yard,
#         (CAST(d.event_time AS BIGINT) - CAST(u.event_time AS BIGINT)) / 1000.0 AS dwell_seconds,
#         u.event_time
#     FROM container_events AS u
#     JOIN container_events AS d
#     ON u.containerId = d.containerId
#     WHERE u.eventType = 'UNLOAD'
#     AND d.status = 'DELIVERED'
#     AND d.event_time > u.event_time
#     """)

#     t_env.execute_sql("""
#     INSERT INTO print_sink
#     SELECT
#         'AVG_DWELL' AS label,
#         yard,
#         AVG(dwell_seconds) AS metric_value,
#         window_start,
#         window_end
#     FROM TABLE(
#         TUMBLE(TABLE container_dwell, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
#     )
#     GROUP BY yard, window_start, window_end
#     """)

#     # Turnaround: keep event_time name on the left (l.event_time)
#     t_env.execute_sql("""
#     CREATE TEMPORARY VIEW container_turnaround AS
#     SELECT
#         l.containerId,
#         l.location AS yard,
#         (CAST(d.event_time AS BIGINT) - CAST(l.event_time AS BIGINT)) / 1000.0 AS turnaround_seconds,
#         l.event_time
#     FROM container_events AS l
#     JOIN container_events AS d
#       ON l.containerId = d.containerId
#     WHERE l.eventType = 'LOAD'
#       AND d.status = 'DELIVERED'
#       AND d.event_time > l.event_time
#     """)

#     t_env.execute_sql("""
#     INSERT INTO print_sink
#     SELECT
#         'TURNAROUND' AS label,
#         yard,
#         AVG(turnaround_seconds) AS metric_value,
#         window_start,
#         window_end
#     FROM TABLE(
#         TUMBLE(TABLE container_turnaround, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
#     )
#     GROUP BY yard, window_start, window_end
#     """)

#     # Delay percent: reuse event_time exactly (no rename/cast)
#     t_env.execute_sql(f"""
#     CREATE TEMPORARY VIEW container_delay_flag AS
#     SELECT
#         containerId,
#         yard,
#         CASE WHEN dwell_seconds > {DELAY_THRESHOLD_SECONDS} THEN 1 ELSE 0 END AS is_delayed,
#         event_time
#     FROM container_dwell
#     """)

#     t_env.execute_sql("""
#     INSERT INTO print_sink
#     SELECT
#         'DELAY_PERCENT' AS label,
#         yard,
#         (CAST(SUM(is_delayed) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)) * 100.0 AS metric_value,
#         window_start,
#         window_end
#     FROM TABLE(
#         TUMBLE(TABLE container_delay_flag, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
#     )
#     GROUP BY yard, window_start, window_end
#     """)

#     # show schema and a preview (optional, helpful for debugging)
#     t_env.execute_sql("DESCRIBE container_dwell").print()
#     # preview will try to read; if you want to see rows, start producer and then do:
#     # t_env.from_path("container_dwell").execute().print()

#     print("Submitting Flink job to print real-time container metrics...")
#     # To actually run the streaming job, use execute() (the INSERT INTOs will submit jobs)
#     # For local testing you can comment/uncomment as needed:
#     # t_env.execute("container_analytics_print")
#     # or just leave the script to submit since INSERT INTO has created pipelines
#     # (behavior depends on your Flink deployment)
# if __name__ == '__main__':
#     main()


# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# KAFKA_BROKERS = "broker:29094"
# KAFKA_TOPIC = "containerEvent"
# DELAY_THRESHOLD_SECONDS = 24 * 3600  # 24 hours

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(2)
#     env.enable_checkpointing(30000)

#     settings = EnvironmentSettings.in_streaming_mode()
#     t_env = StreamTableEnvironment.create(env, environment_settings=settings)

#     # --- 1️⃣ SOURCE: Kafka container events table ---
#     t_env.execute_sql(f"""
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
#         event_time AS `timestamp`,
#         WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
#     ) WITH (
#         'connector' = 'kafka',
#         'topic' = '{KAFKA_TOPIC}',
#         'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
#         'properties.group.id' = 'container-analytics-group',
#         'scan.startup.mode' = 'earliest-offset',
#         'format' = 'json',
#         'json.ignore-parse-errors' = 'true'
#     )
#     """)

#     # --- 2️⃣ DEBUG SINK (prints to terminal) ---
#     t_env.execute_sql("""
#     CREATE TABLE print_sink (
#         label STRING,
#         yard STRING,
#         metric_value DOUBLE,
#         window_start TIMESTAMP_LTZ(3),
#         window_end TIMESTAMP_LTZ(3)
#     ) WITH (
#         'connector' = 'print'
#     )
#     """)

#     # --- 3️⃣ DWELL TIME CALCULATION ---
#     t_env.execute_sql("""
#     CREATE TEMPORARY VIEW container_dwell AS
#     SELECT
#         u.containerId,
#         u.location AS yard,
#         (EXTRACT(EPOCH FROM d.event_time) - EXTRACT(EPOCH FROM u.event_time)) AS dwell_seconds,
#         u.event_time
#     FROM (
#         SELECT containerId, event_time, eventType, location
#         FROM container_events
#         WHERE eventType = 'UNLOAD'
#     ) AS u
#     JOIN (
#         SELECT containerId, event_time, status
#         FROM container_events
#         WHERE status = 'DELIVERED'
#     ) AS d
#     ON u.containerId = d.containerId
#     WHERE d.event_time > u.event_time
#     AND d.event_time BETWEEN u.event_time AND u.event_time + INTERVAL '7' YEAR                     
#     """)

#     # --- 4️⃣ INSERT AVERAGE DWELL TIME METRIC ---
#     result = t_env.execute_sql("""
#     INSERT INTO print_sink
#     SELECT
#         'AVG_DWELL' AS label,
#         yard,
#         AVG(dwell_seconds) AS metric_value,
#         TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
#         TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end
#     FROM container_dwell
#     GROUP BY
#         yard,
#         TUMBLE(event_time, INTERVAL '1' MINUTE)    
#     """)

    # # --- 5️⃣ TURNAROUND TIME CALCULATION ---
    # t_env.execute_sql("""
    # CREATE TEMPORARY VIEW container_turnaround AS
    # SELECT
    #     l.containerId,
    #     l.location AS yard,
    #     (EXTRACT(EPOCH FROM d.event_time) - EXTRACT(EPOCH FROM l.event_time)) AS turnaround_seconds,
    #     l.event_time
    # FROM (
    #     SELECT containerId, event_time, eventType, location
    #     FROM container_events
    #     WHERE eventType = 'LOAD'
    # ) AS l
    # JOIN (
    #     SELECT containerId, event_time, status
    #     FROM container_events
    #     WHERE status = 'DELIVERED'
    # ) AS d
    # ON l.containerId = d.containerId
    # WHERE d.event_time > l.event_time
    # """)

    # # --- 6️⃣ INSERT AVERAGE TURNAROUND TIME METRIC ---
    # t_env.execute_sql("""
    # INSERT INTO print_sink
    # SELECT
    #     'TURNAROUND' AS label,
    #     yard,
    #     AVG(turnaround_seconds) AS metric_value,
    #     window_start,
    #     window_end
    # FROM TABLE(
    #     TUMBLE(TABLE container_turnaround, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
    # )
    # GROUP BY yard, window_start, window_end
    # """)

    # # --- 7️⃣ DELAY PERCENTAGE CALCULATION ---
    # t_env.execute_sql(f"""
    # CREATE TEMPORARY VIEW container_delay_flag AS
    # SELECT
    #     containerId,
    #     yard,
    #     CASE WHEN dwell_seconds > {DELAY_THRESHOLD_SECONDS} THEN 1 ELSE 0 END AS is_delayed,
    #     event_time
    # FROM container_dwell
    # """)

    # # --- 8️⃣ INSERT DELAY PERCENT METRIC ---
    # t_env.execute_sql("""
    # INSERT INTO print_sink
    # SELECT
    #     'DELAY_PERCENT' AS label,
    #     yard,
    #     (CAST(SUM(is_delayed) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)) * 100.0 AS metric_value,
    #     window_start,
    #     window_end
    # FROM TABLE(
    #     TUMBLE(TABLE container_delay_flag, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
    # )
    # GROUP BY yard, window_start, window_end
    # """)

    # --- 9️⃣ OPTIONAL DEBUG: Check schema ---
    #     t_env.execute_sql("DESCRIBE container_events").print()
    #     t_env.execute_sql("DESCRIBE print_sink").print()
    #     t_env.execute_sql("DESCRIBE container_dwell").print()
    #     # t_env.execute_sql("DESCRIBE container_turnaround").print()
    #     # t_env.execute_sql("DESCRIBE container_delay_flag").print()

    #     # result = t_env.sql_query("SELECT * FROM container_dwell")
    #     # result.execute().print()
    #     result.print()


    #     print("✅ Flink job submitted. Watch terminal for real-time printed metrics...")

    # if __name__ == '__main__':
    #     main()

from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # ---------------- Kafka Source Table ----------------
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
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '10' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'containerEvent',
        'properties.bootstrap.servers' = 'broker:29094',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'scan.startup.mode' = 'earliest-offset'
    )
    """)

    # ---------------- Console Sink ----------------
    t_env.execute_sql("""
    CREATE TABLE event_throughput (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        eventType STRING,
        total_events BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """)

    # ---------------- Analytics Query ----------------
    t_env.execute_sql("""
    INSERT INTO event_throughput
    SELECT
        window_start,
        window_end,
        eventType,
        COUNT(*) AS total_events
    FROM TABLE(
        TUMBLE(TABLE container_events, DESCRIPTOR(`timestamp`), INTERVAL '1' MINUTE)
    )
    GROUP BY window_start, window_end, eventType
    """)

if __name__ == "__main__":
    main()
