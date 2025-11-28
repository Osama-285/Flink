import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common import Types
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, Schema, DataTypes


def main():
    # -------------------------------------------------------------------
    # 1. Setup
    # -------------------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

    # -------------------------------------------------------------------
    # 2. Kafka Source (DataStream → Table)
    # -------------------------------------------------------------------
    json_schema = JsonRowDeserializationSchema.builder() \
        .type_info(
            Types.ROW_NAMED(
                ["txn_id", "card_number", "amount", "merchant", "ts"],
                [Types.INT(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.LONG()]
            )
        ).build()

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29094") \
        .set_topics("transaction") \
        .set_group_id("transaction-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_schema) \
        .build()

    txn_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="KafkaTxnSource"
    )

    # Register table view for transactions
    t_env.create_temporary_view(
        "transactions",
        txn_stream,
        Schema.new_builder()
            .column("txn_id", DataTypes.INT())
            .column("card_number", DataTypes.STRING())
            .column("amount", DataTypes.FLOAT())
            .column("merchant", DataTypes.STRING())
            .column("ts", DataTypes.BIGINT())   # epoch millis
            .column_by_expression("ts_ltz", "TO_TIMESTAMP_LTZ(ts, 3)")
            .watermark("ts_ltz", "ts_ltz - INTERVAL '5' SECOND")
            .build()
    )

    # -------------------------------------------------------------------
    # 3. Fraud ratio with JOIN + Tumbling Window (inline VALUES instead of table)
    # -------------------------------------------------------------------
    query = """
    SELECT
        TUMBLE_START(ts_ltz, INTERVAL '30' SECOND) AS window_start,
        COUNT(*) AS total_txns,
        SUM(CASE WHEN risk_profiles.risk = 'high' THEN 1 ELSE 0 END) AS high_risk_txns,
        CAST(
            CASE WHEN COUNT(*) > 0
                 THEN SUM(CASE WHEN risk_profiles.risk = 'high' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
                 ELSE 0
            END AS DOUBLE
        ) AS fraud_ratio
    FROM transactions t
    LEFT JOIN (
        VALUES
          ('4111-1111-1111-1111', 'high'),
          ('4000-0000-0000-0241', 'medium'),
          ('4000-0000-0000-0003', 'low')
    ) AS risk_profiles(card_number, risk)
    ON t.card_number = risk_profiles.card_number
    GROUP BY TUMBLE(ts_ltz, INTERVAL '30' SECOND)
    """

    fraud_metrics = t_env.sql_query(query)

    # -------------------------------------------------------------------
    # 4. Print Sink
    # -------------------------------------------------------------------
    t_env.execute_sql("""
    CREATE TEMPORARY TABLE print_sink (
        window_start TIMESTAMP(3),
        total_txns BIGINT,
        high_risk_txns BIGINT,
        fraud_ratio DOUBLE
    ) WITH (
      'connector' = 'print'
    )
    """)

    fraud_metrics.execute_insert("print_sink").wait()


if __name__ == "__main__":
    main()
