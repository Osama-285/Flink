# import json
# from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
# from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext

# ITEM_METADATA = {
#     500: {"category": "electronics"},
#     501: {"category": "books"},
#     502: {"category": "sports"},
#     503: {"category": "fashion"},
#     504: {"category": "home"}
# }


# class RecommendationProcessFunction(KeyedProcessFunction):

#     def open(self, runtime_context: RuntimeContext):
#         # Per-user state: list of recent categories viewed
#         self.recent_categories_state = runtime_context.get_state(
#             name="recent_categories",
#             state_type=Types.LIST(Types.STRING())
#         )

#     def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
#         event = json.loads(value)

#         user_id = event["user_id"]
#         item_id = event["item_id"]

#         # Look up category from item metadata
#         category = ITEM_METADATA.get(item_id, {"category": "unknown"})["category"]

#         # Get state
#         recent_categories = self.recent_categories_state.value()
#         if recent_categories is None:
#             recent_categories = []

#         # Update state (keep last 3 categories)
#         recent_categories.append(category)
#         recent_categories = recent_categories[-3:]
#         self.recent_categories_state.update(recent_categories)

#         # Generate simple recommendation (most recent category)
#         recommendation = {
#             "user_id": user_id,
#             "recommended_category": recent_categories[-1],
#             "reason": f"Based on your interest in {recent_categories[-1]}"
#         }

#         return json.dumps(recommendation)


# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
#     env.set_parallelism(1)

#     # Kafka consumer
#     consumer_props = {
#         'bootstrap.servers': 'localhost:9094',
#         'group.id': 'flink-recommender'
#     }

#     kafka_consumer = FlinkKafkaConsumer(
#         topics='user_activity',
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     # Kafka producer
#     kafka_producer = FlinkKafkaProducer(
#         topic='recommendations',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'localhost:9094'}
#     )

#     ds = env.add_source(kafka_consumer)

#     # Key by user_id → process events → output recommendations
#     recommendations = ds \
#         .key_by(lambda e: json.loads(e)["user_id"], key_type=Types.INT()) \
#         .process(RecommendationProcessFunction(), output_type=Types.STRING())

#     recommendations.add_sink(kafka_producer)

#     env.execute("Real-Time Recommendation Engine")


# if __name__ == '__main__':
#     main()


# import json
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common import Types
# from pyflink.datastream.connectors.kafka import (
#     KafkaSource,
#     KafkaSink,
#     KafkaRecordSerializationSchema
# )
# from pyflink.table import StreamTableEnvironment, EnvironmentSettings
# from pyflink.common.watermark_strategy import WatermarkStrategy
# from pyflink.datastream.state import ListStateDescriptor
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext


# # Example item metadata
# ITEM_METADATA = {
#     500: {"category": "electronics"},
#     501: {"category": "books"},
#     502: {"category": "sports"},
#     503: {"category": "fashion"},
#     504: {"category": "home"}
# }


# class RecommendationProcessFunction(KeyedProcessFunction):

#     def open(self, runtime_context: RuntimeContext):
#         descriptor = ListStateDescriptor(
#             "recent_categories",
#             Types.STRING()  # element type
#         )
#         self.recent_categories_state = runtime_context.get_list_state(descriptor)

#     def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
#         event = json.loads(value)

#         user_id = event["user_id"]
#         item_id = event["item_id"]

#         category = ITEM_METADATA.get(item_id, {"category": "unknown"})["category"]

#         # Fetch current state
#         recent_categories = list(self.recent_categories_state.get())

#         # Update state
#         recent_categories.append(category)
#         recent_categories = recent_categories[-3:]

#         self.recent_categories_state.update(recent_categories)

#         recommendation = {
#             "user_id": user_id,
#             "recommended_category": recent_categories[-1],
#             "reason": f"Based on your interest in {recent_categories[-1]}"
#         }

#         yield json.dumps(recommendation)
#         yield (user_id, recent_categories[-1], f"Based on your interest in {recent_categories[-1]}")


# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)
#     settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
#     t_env = StreamTableEnvironment.create(env, environment_settings=settings)
#     # Kafka source (new API)
#     source = (
#         KafkaSource.builder()
#         .set_bootstrap_servers("broker:29094")
#         .set_topics("user_activity")
#         .set_group_id("flink-recommender")
#         .set_value_only_deserializer(SimpleStringSchema())
#         .build()
#     )

#     # Kafka sink (new API)
#     sink = (
#         KafkaSink.builder()
#         .set_bootstrap_servers("broker:29094")
#         .set_record_serializer(
#             KafkaRecordSerializationSchema.builder()
#             .set_topic("recommendations")
#             .set_value_serialization_schema(SimpleStringSchema())
#             .build()
#         )
#         .build()
#     )

#     ds = env.from_source(source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="KafkaSource")

#     recommendations = (
#         ds.key_by(lambda e: json.loads(e)["user_id"], key_type=Types.INT())
#         .process(RecommendationProcessFunction(), output_type=Types.STRING())
#     )

#     recommendations.sink_to(sink)
#     table = t_env.from_data_stream(
#     recommendations,
#     schema=["user_id INT", "recommended_category STRING", "reason STRING"]
#     )

#     t_env.execute_sql("""
#         CREATE TABLE recommendations (
#             recommendation STRING
#         ) WITH (
#             'connector' = 'jdbc',
#             'url' = 'jdbc:postgresql://host.docker.internal:5434/maindb',
#             'table-name' = 'recommendations',
#             'username' = 'admin',
#             'password' = 'admin123',
#             'driver' = 'org.postgresql.Driver'
#         )
#     """)
#     table.execute("recommendations").wait()
#     env.execute("Real-Time Recommendation Engine (Flink 2.x)")


# if __name__ == "__main__":
#     main()

# import json
# from pyflink.common import Row, Types
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import StreamTableEnvironment, EnvironmentSettings
# from pyflink.datastream.connectors.kafka import (
#     KafkaSource, KafkaSink, KafkaRecordSerializationSchema
# )
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.formats.json import JsonRowDeserializationSchema
# from pyflink.datastream.state import ListStateDescriptor
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common.watermark_strategy import WatermarkStrategy


# # Example item metadata
# ITEM_METADATA = {
#     380: {"category": "electronics"},
#     135: {"category": "books"},
#     502: {"category": "sports"},
#     503: {"category": "fashion"},
#     504: {"category": "home"},
#     585: {"category": "home"},
# }


# # -------------------------------
# # Process Function
# # -------------------------------
# class RecommendationProcessFunction(KeyedProcessFunction):

#     def open(self, runtime_context: RuntimeContext):
#         descriptor = ListStateDescriptor(
#             "recent_categories",
#             Types.STRING()
#         )
#         self.recent_categories_state = runtime_context.get_list_state(descriptor)

#     def process_element(self, event, ctx: 'KeyedProcessFunction.Context'):
#         user_id = event["user_id"]
#         item_id = event["item_id"]

#         category = ITEM_METADATA.get(item_id, {"category": "unknown"})["category"]

#         # Fetch current state
#         recent_categories = list(self.recent_categories_state.get())

#         # Update state
#         recent_categories.append(category)
#         recent_categories = recent_categories[-3:]
#         self.recent_categories_state.update(recent_categories)

#         # Build recommendation
#         reason = f"Based on your interest in {recent_categories[-1]}"

#         # Yield structured Row (for Postgres sink & Table API)
#         yield Row(user_id, recent_categories[-1], reason)


# # -------------------------------
# # Main Job
# # -------------------------------
# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)
#     settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
#     t_env = StreamTableEnvironment.create(env, environment_settings=settings)

#     # -------------------------------
#     # Kafka Source (typed JSON → Row)
#     # -------------------------------
#     deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
#         Types.ROW_NAMED(
#             ["user_id", "item_id", "event_type", "timestamp", "device", "location"],
#             [Types.INT(), Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
#         )
#     ).build()

#     source = (
#         KafkaSource.builder()
#         .set_bootstrap_servers("broker:29094")
#         .set_topics("user_activity")
#         .set_group_id("flink-recommender")
#         .set_value_only_deserializer(deserialization_schema)
#         .build()
#     )

#     ds = env.from_source(
#         source,
#         watermark_strategy=WatermarkStrategy.no_watermarks(),
#         source_name="KafkaSource"
#     )

#     # -------------------------------
#     # Process: Generate Recommendations
#     # -------------------------------
#     recommendations = (
#         ds.key_by(lambda e: e["user_id"], key_type=Types.INT())
#         .process(
#             RecommendationProcessFunction(),
#             output_type=Types.ROW_NAMED(
#                 ["user_id", "recommended_category", "reason"],
#                 [Types.INT(), Types.STRING(), Types.STRING()]
#             )
#         )
#     )

#     # -------------------------------
#     # Kafka Sink (JSON for consumers)
#     # -------------------------------
#     kafka_sink = (
#         KafkaSink.builder()
#         .set_bootstrap_servers("broker:29094")
#         .set_record_serializer(
#             KafkaRecordSerializationSchema.builder()
#             .set_topic("recommendations")
#             .set_value_serialization_schema(SimpleStringSchema())
#             .build()
#         )
#         .build()
#     )

#     # Convert Row → JSON before writing to Kafka
#     recommendations.map(
#         lambda r: json.dumps({
#             "user_id": r[0],
#             "recommended_category": r[1],
#             "reason": r[2]
#         }),
#         output_type=Types.STRING()
#     ).sink_to(kafka_sink)

#     # -------------------------------
#     # PostgreSQL Sink
#     # -------------------------------
#     # Convert DataStream → Table
#     table = t_env.from_data_stream(recommendations)

#     # Create Postgres table
#     t_env.execute_sql("""
#         CREATE TABLE recommendations_pg (
#             user_id INT,
#             recommended_category STRING,
#             reason STRING
#         ) WITH (
#             'connector' = 'jdbc',
#             'url' = 'jdbc:postgresql://host.docker.internal:5434/maindb',
#             'table-name' = 'recommendations',
#             'username' = 'admin',
#             'password' = 'admin123',
#             'driver' = 'org.postgresql.Driver'
#         )
#     """)

#     # Insert structured rows
#     table.execute_insert("recommendations_pg")

#     # -------------------------------
#     # Run the job
#     # -------------------------------
#     env.execute("Real-Time Recommendation Engine (Flink 2.x)")


# if __name__ == "__main__":
#     main()


import json
from pyflink.common import Row, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.state import ListStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.watermark_strategy import WatermarkStrategy


# -------------------------------
# Example item metadata
# -------------------------------
ITEM_METADATA = {
    500: {"category": "electronics"},
    501: {"category": "books"},
    502: {"category": "sports"},
    503: {"category": "fashion"},
    504: {"category": "home"}
}


# -------------------------------
# Process Function
# -------------------------------
class RecommendationProcessFunction(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        descriptor = ListStateDescriptor(
            "recent_categories",
            Types.STRING()
        )
        self.recent_categories_state = runtime_context.get_list_state(descriptor)

    def process_element(self, event, ctx: 'KeyedProcessFunction.Context'):
        user_id = event["user_id"]
        item_id = event["item_id"]

        category = ITEM_METADATA.get(item_id, {"category": "unknown"})["category"]

        recent_categories = list(self.recent_categories_state.get())
        recent_categories.append(category)
        recent_categories = recent_categories[-3:]
        self.recent_categories_state.update(recent_categories)

        reason = f"Based on your interest in {recent_categories[-1]}"

        # Yield Row: clean typed structure
        yield Row(user_id, recent_categories[-1], reason)


# -------------------------------
# Helper: Kafka Sink Factory
# -------------------------------
def create_kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers("broker:29094")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


# -------------------------------
# Main Job
# -------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Kafka source (typed JSON → Row)
    deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
        Types.ROW_NAMED(
            ["user_id", "item_id", "event_type", "timestamp", "device", "location"],
            [Types.INT(), Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
        )
    ).build()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker:29094")
        .set_topics("user_activity")
        .set_group_id("flink-recommender")
        .set_value_only_deserializer(deserialization_schema)
        .build()
    )

    ds = env.from_source(
        source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="KafkaSource"
    )

    # Process recommendations
    recommendations = (
        ds.key_by(lambda e: e["user_id"], key_type=Types.INT())
        .process(
            RecommendationProcessFunction(),
            output_type=Types.ROW_NAMED(
                ["user_id", "recommended_category", "reason"],
                [Types.INT(), Types.STRING(), Types.STRING()]
            )
        )
    )

    # -------------------------------
    # Sink to Kafka (JSON for consumers)
    # -------------------------------
    json_stream = recommendations.map(
        lambda r: json.dumps({
            "user_id": r[0],
            "recommended_category": r[1],
            "reason": r[2]
        }),
        output_type=Types.STRING()
    )
    json_stream.sink_to(create_kafka_sink("recommendations"))

    # -------------------------------
    # Sink to Postgres (structured rows)
    # -------------------------------
    table = t_env.from_data_stream(recommendations)

    t_env.execute_sql("""
        CREATE TABLE recommendations_pg (
            user_id INT,
            recommended_category STRING,
            reason STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://host.docker.internal:5434/maindb',
            'table-name' = 'recommendations',
            'username' = 'admin',
            'password' = 'admin123',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    table.execute_insert("recommendations_pg")

    # -------------------------------
    # Run the job
    # -------------------------------
    env.execute("Real-Time Recommendation Engine (Flink 2.x)")


if __name__ == "__main__":
    main()
