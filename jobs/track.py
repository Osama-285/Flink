# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
# from pyflink.common.serialization import SimpleStringSchema
# import json
# from pyflink.common import WatermarkStrategy


# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     # -------------------- Kafka Source (New API) --------------------
#     source = (
#         KafkaSource.builder()
#         .set_bootstrap_servers("broker:29094")
#         .set_topics("containerEvent")
#         .set_group_id("flink-container-consumer")
#         .set_starting_offsets(KafkaOffsetsInitializer.earliest())
#         .set_value_only_deserializer(SimpleStringSchema())
#         .build()
#     )

#     stream = env.from_source(
#         source,
#         watermark_strategy=WatermarkStrategy.no_watermarks(),  # optional
#         source_name="Kafka Ship Source"
#     )

#     # -------------------- Safe JSON Parse --------------------
#     def safe_parse(value):
#         try:
#             data = json.loads(value)
#             return data
#         except Exception as e:
#             print(f"❌ Skipping invalid record: {value} ({e})")
#             return None

#     parsed_stream = stream.map(safe_parse).filter(lambda x: x is not None)
#     parsed_stream.print()

#     env.execute("Kafka Safe JSON Stream")

# if __name__ == "__main__":
#     main()

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.functions import MapFunction, RuntimeContext, ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
import json


class JsonValidator(MapFunction):
    """Validate incoming Kafka messages as JSON."""

    def open(self, runtime_context: RuntimeContext):
        self.valid_counter = 0
        self.invalid_counter = 0

    def map(self, value):
        try:
            json.loads(value)
            self.valid_counter += 1
            print(f"✅ Valid JSON #{self.valid_counter}: {value}")
            return ("valid", value)
        except Exception:
            self.invalid_counter += 1
            print(f"❌ Invalid JSON #{self.invalid_counter}: {value}")
            return ("invalid", value)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # -------------------- Kafka Source --------------------
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker:29094")
        .set_topics("shipEvent")
        .set_group_id("flink-json-checker")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    # -------------------- Apply JSON Validation --------------------
    validated_stream = stream.map(
        JsonValidator(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )

    # -------------------- Print Summary --------------------
    def print_summary(value):
        status, msg = value
        if status == "valid":
            print(f"VALID: {msg}")
        else:
            print(f"INVALID: {msg}")
        return value

    validated_stream.map(print_summary)

    env.execute("JSON Validation Console Printer")


if __name__ == "__main__":
    main()
