
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

KAFKA_BOOTSTRAP = "broker:29094" 
PRODUCT_TOPIC = "productEvent"
CONTAINER_TOPIC = "containerEvent"
GROUP_ID = "flink-container-product-join-group"

def main():
    # set up env
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Common Kafka options (adjust as needed)
    kafka_options = {
        'connector': 'kafka',
        'properties.bootstrap.servers': KAFKA_BOOTSTRAP,
        'properties.group.id': GROUP_ID,
        'scan.startup.mode': 'earliest-offset',
        'format': 'json'   # using built-in json format
    }

    # DDL for product events (your provided schema)
    # timestamp is input as ISO8601 string like "2025-10-24T12:34:56Z"
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
      WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
    ) WITH (
      'connector' = 'kafka',
      'topic' = '{PRODUCT_TOPIC}',
      'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
      'properties.group.id' = '{GROUP_ID}_prod',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json',
      'json.timestamp-format.standard' = 'ISO-8601',
      'json.ignore-parse-errors' = 'true',
      'json.fail-on-missing-field' = 'false'
    )
    """)

    # DDL for container events. Assumed fields; adjust if your containerEvent schema differs.
    t_env.execute_sql(f"""
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
        event_time AS `timestamp`,
      WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
    ) WITH (
      'connector' = 'kafka',
      'topic' = '{CONTAINER_TOPIC}',
      'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
      'properties.group.id' = '{GROUP_ID}_cont',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json',
      'json.timestamp-format.standard' = 'ISO-8601',
      'json.ignore-parse-errors' = 'true',
      'json.fail-on-missing-field' = 'false'
    )
    """)

    # Print sink to stdout (for local testing)
    t_env.execute_sql("""
    CREATE TABLE print_sink (
      productId STRING,
      containerId STRING,
      shipId STRING,
      productName STRING,
      category STRING,
      quantity INT,
      weightKg DOUBLE,
      productStatus STRING,
      product_ts STRING,
      containerLocation STRING,
      containerSeal STRING,
      containerStatus STRING,
      container_ts STRING,
      product_event_time TIMESTAMP_LTZ(3),
      container_event_time TIMESTAMP_LTZ(3)
    ) WITH (
      'connector' = 'print'
    )
    """)

    # Time-bounded join: match events where containerId is same and container event time is within
    # [product_event_time - 5 minutes, product_event_time + 5 minutes]
    # Adjust interval as required (here +/- 5 minutes)
    insert_sql = """
    INSERT INTO print_sink
        SELECT
        p.productId,
        p.containerId,
        p.shipId,
        p.name AS productName,
        p.category,
        p.quantity,
        p.weightKg,
        p.status AS productStatus,
        CAST(p.`timestamp` AS STRING) AS product_ts,
        c.location AS containerLocation,
        c.sealNumber AS containerSeal,
        c.status AS containerStatus,
        CAST(c.`timestamp` AS STRING) AS container_ts,
        CAST(p.event_time AS TIMESTAMP_LTZ(3)) AS product_event_time,
        CAST(c.event_time AS TIMESTAMP_LTZ(3)) AS container_event_time
    FROM product_events AS p
    JOIN container_events AS c
        ON p.containerId = c.containerId
        AND c.event_time BETWEEN p.event_time - INTERVAL '5' MINUTE AND p.event_time + INTERVAL '5' MINUTE
    """

    # execute insert (this will start the streaming job)
    t_env.execute_sql(insert_sql)
    # Note: in some PyFlink versions you may want to call env.execute("job-name"),
    # but t_env.execute_sql on a DML INSERT typically submits the job.

if __name__ == "__main__":
    main()
