# import json
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import (
#     KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
# )
# from pyflink.common.watermark_strategy import WatermarkStrategy
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.common.typeinfo import Types
# from pyflink.table import StreamTableEnvironment, EnvironmentSettings, Schema, DataTypes
# from pyflink.table.expressions import col
# from pyflink.common import Row


# def main():
#     # ----------------------------
#     # Setup Environments
#     # ----------------------------
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
#     t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

#     BOOTSTRAP = "broker:29094"
#     TXN_TOPIC = "transactions"
#     METRICS_TOPIC = "metrics"

#     # ----------------------------
#     # Kafka Source
#     # ----------------------------
#     kafka_source = (
#         KafkaSource.builder()
#         .set_bootstrap_servers(BOOTSTRAP)
#         .set_topics(TXN_TOPIC)
#         .set_group_id("fraud-group")
#         .set_starting_offsets(KafkaOffsetsInitializer.earliest())
#         .set_value_only_deserializer(SimpleStringSchema())
#         .build()
#     )

#     ds = env.from_source(
#         kafka_source,
#         watermark_strategy=WatermarkStrategy.no_watermarks(),
#         source_name="KafkaSource"
#     )

#     # ----------------------------
#     # Parse JSON → Row with named fields
#     # ----------------------------
#     txn_stream = ds.map(
#         lambda x: json.loads(x),
#         output_type=Types.MAP(Types.STRING(), Types.STRING())
#     ).map(
#         lambda d: Row(
#             txn_id=d["txn_id"],
#             card_number=d["card_number"],
#             amount=float(d["amount"]),
#             merchant=d["merchant"],
#             ts=int(d["ts"])
#         ),
#         output_type=Types.ROW_NAMED(
#             ["txn_id", "card_number", "amount", "merchant", "ts"],
#             [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.STRING(), Types.LONG()]
#         )
#     )

#     # ----------------------------
#     # Convert DataStream → Table
#     # ----------------------------
#     txn_table = t_env.from_data_stream(
#         txn_stream,
#         Schema.new_builder()
#             .column("txn_id", DataTypes.STRING())
#             .column("card_number", DataTypes.STRING())
#             .column("amount", DataTypes.DOUBLE())
#             .column("merchant", DataTypes.STRING())
#             .column("ts", DataTypes.BIGINT())
#             .build()
#     )
#     t_env.create_temporary_view("Transactions", txn_table)

#     # ----------------------------
#     # Risk Profiles (static table)
#     # ----------------------------
#     risk_profiles = t_env.from_elements(
#         [(f"card_{i}", "HIGH" if i % 5 == 0 else "LOW") for i in range(1, 101)],
#         ["card_number", "risk_level"]
#     )
#     t_env.create_temporary_view("RiskProfiles", risk_profiles)

#     # ----------------------------
#     # Fraud Detection Join + Aggregation
#     # ----------------------------
#     transactions = t_env.from_path("Transactions") \
#         .alias("txn_txn_id", "txn_card_number", "txn_amount", "txn_merchant", "txn_ts")

#     risks = t_env.from_path("RiskProfiles") \
#         .alias("risk_card_number", "risk_level")

#     joined = transactions.join(risks) \
#         .where(col("txn_card_number") == col("risk_card_number")) \
#         .select(col("txn_card_number").alias("card_number"),
#                 col("txn_txn_id").alias("txn_id"))

#     metrics = joined.group_by(col("card_number")).select(
#         col("card_number"),
#         col("txn_id").count.alias("txn_count")
#     )

#     # ----------------------------
#     # Kafka Sink for Metrics
#     # ----------------------------
#     kafka_sink = (
#         KafkaSink.builder()
#         .set_bootstrap_servers(BOOTSTRAP)
#         .set_record_serializer(
#             KafkaRecordSerializationSchema.builder()
#             .set_topic(METRICS_TOPIC)
#             .set_value_serialization_schema(SimpleStringSchema())
#             .build()
#         )
#         .build()
#     )

#     # ----------------------------
#     # Print + Sink (use changelog stream!)
#     # ----------------------------
#     (
#         t_env.to_changelog_stream(metrics)
#         .map(lambda row: json.dumps({"card_number": str(row[0]), "txn_count": int(row[1])}),
#             output_type=Types.STRING())
#         .map(lambda s: (print(f"📊 METRIC OUT: {s}") or s), output_type=Types.STRING())
#         .sink_to(kafka_sink)
#     )

#     # ----------------------------
#     # Execute Job
#     # ----------------------------
#     env.execute("fraud-detection-job")


# if __name__ == "__main__":
#     main()

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
    # t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
    # json_schema = JsonRowDeserializationSchema.builder() \
    # .type_info(
    #     DataTypes.ROW([
    #         DataTypes.FIELD("txn_id", DataTypes.INT()),
    #         DataTypes.FIELD("card_number", DataTypes.STRING()),
    #         DataTypes.FIELD("amount", DataTypes.FLOAT()),
    #         DataTypes.FIELD("merchant", DataTypes.STRING()),
    #         DataTypes.FIELD("ts", DataTypes.BIGINT())
    #     ])
    # ).build()
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

    txn_stream.print()

# 6. Execute the job
    env.execute("Txn Kafka Consumer")





if __name__ == "__main__":
    main()