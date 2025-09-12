from pyflink.table import EnvironmentSettings, TableEnvironment

def fraud_detection():
    # --------------------------------------------------------------------
    # 1) Setup Flink environment
    # --------------------------------------------------------------------
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    BOOTSTRAP = "broker:29094"

    # --------------------------------------------------------------------
    # 2) Source: Transactions from Kafka (JSON)
    # --------------------------------------------------------------------
    t_env.execute_sql(f"""
    CREATE TABLE Transactions (
      transaction_id STRING,
      account_id STRING,
      amount DOUBLE,
      city STRING,
      timestamp_ms BIGINT,
      ts AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),
      WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'transactions',
      'properties.bootstrap.servers' = '{BOOTSTRAP}',
      'properties.group.id' = 'flink-fraud',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
    )
    """)

    # --------------------------------------------------------------------
    # 3) Sink: Fraud alerts into Kafka (upsert-kafka JSON)
    # --------------------------------------------------------------------
    t_env.execute_sql(f"""
    CREATE TABLE FraudAlerts (
      account_id STRING,
      window_start TIMESTAMP_LTZ(3),
      cnt BIGINT,
      PRIMARY KEY (account_id, window_start) NOT ENFORCED
    ) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'fraud_alerts',
      'properties.bootstrap.servers' = '{BOOTSTRAP}',
      'key.format' = 'json',
      'value.format' = 'json'
    )
    """)

    # --------------------------------------------------------------------
    # 4) Unified Fraud Detection Query (merge both rules)
    # --------------------------------------------------------------------
    t_env.execute_sql("""
    INSERT INTO FraudAlerts
    SELECT account_id, window_start, cnt
    FROM (
        SELECT
          account_id,
          window_start,
          COUNT(*) AS cnt
        FROM TABLE(
          TUMBLE(TABLE Transactions, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
        )
        WHERE amount > 5000
        GROUP BY account_id, window_start
        HAVING COUNT(*) >= 3

        UNION ALL

        SELECT
          account_id,
          window_start,
          COUNT(DISTINCT city) AS cnt
        FROM TABLE(
          TUMBLE(TABLE Transactions, DESCRIPTOR(ts), INTERVAL '4' MINUTES)
        )
        GROUP BY account_id, window_start
        HAVING COUNT(DISTINCT city) > 1
    )
    """)

if __name__ == "__main__":
    fraud_detection()
