import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

from pyflink.table import StreamTableEnvironment, EnvironmentSettings, Schema, DataTypes
from pyflink.table.expressions import col
from pyflink.common import Row

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
    risk_profiles = [
    ("4111-1111-1111-1111", "high"),
    ("4000-0000-0000-0241", "medium"),
    ("4000-0000-0000-0003", "low")
    ]
    risk_dict = dict(risk_profiles)

    json_schema = JsonRowDeserializationSchema.builder() \
    .type_info(
        Types.ROW_NAMED(
            ["txn_id", "card_number", "amount", "merchant", "ts"],
            [Types.INT(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.LONG()]
        )
    ).build()

    print("Json Schema: ",json_schema)

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
    def enrich_with_risk(txn):
        card_number = txn['card_number'] # position 1 in the tuple
        risk = risk_dict.get(card_number, "unknown")
        # return txn + (risk,)
        return Row(txn_id=txn['txn_id'],
               card_number=txn['card_number'],
               amount=txn['amount'],
               merchant=txn['merchant'],
               ts=txn['ts'],
               risk=risk)

    # txn_stream.print()
    output_type = Types.ROW_NAMED(
        ["txn_id", "card_number", "amount", "merchant", "ts", "risk"],
        [Types.INT(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.LONG(), Types.STRING()]
    )
    enriched = txn_stream.map(enrich_with_risk, output_type=output_type)

    # enriched.print()
    t_env.create_temporary_view(
        "enriched_txn",
        enriched,
        Schema.new_builder()
            .column("txn_id", DataTypes.INT())
            .column("card_number", DataTypes.STRING())
            .column("amount", DataTypes.FLOAT())
            .column("merchant", DataTypes.STRING())
            .column("ts", DataTypes.BIGINT())   # epoch millis
            .column("risk", DataTypes.STRING())
            # event-time column
            .column_by_expression("ts_ltz", "TO_TIMESTAMP_LTZ(ts, 3)")
            .watermark("ts_ltz", "ts_ltz - INTERVAL '5' SECOND")
            .build()
    )

    query = """
    SELECT
        TUMBLE_START(ts_ltz, INTERVAL '30' SECOND) AS window_start,
        COUNT(*) AS total_txns,
        SUM(CASE WHEN risk = 'high' THEN 1 ELSE 0 END) AS high_risk_txns,
        CAST(SUM(CASE WHEN risk = 'high' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_ratio
    FROM enriched_txn
    GROUP BY TUMBLE(ts_ltz, INTERVAL '30' SECOND)
    """
    
    fraud_metrics = t_env.sql_query(query)
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


    fraud_metrics.execute_insert("print_sink")
# 6. Execute the job
    env.execute("Txn Kafka Consumer")





if __name__ == "__main__":
    main()