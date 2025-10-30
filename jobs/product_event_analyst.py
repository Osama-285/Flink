from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration, Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import FlatMapFunction
from pyflink.datastream import CheckpointingMode
from datetime import datetime, timedelta
from pyflink.common import WatermarkStrategy
import json
import os
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder

# ==============================
# CONFIGURATION
# ==============================
OUTPUT_DIR = os.path.join(os.getcwd(), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
VALID_FILE = os.path.join(OUTPUT_DIR, "validated_output.json")
INVALID_FILE = os.path.join(OUTPUT_DIR, "invalid_records.json")

KAFKA_BROKERS = "broker:29094"
KAFKA_TOPIC = "productEvent"

VALID_CATEGORIES = {"Food", "Automobile", "Electronics", "Furniture", "Clothing"}
VALID_STATUSES = {"IN_CONTAINER", "UNLOADED", "DELIVERED"}

# ==============================
# FLINK ENVIRONMENT
# ==============================
config = Configuration()
env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
env.set_parallelism(2)
env.enable_checkpointing(10_000)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# ==============================
# KAFKA SOURCE (New API)
# ==============================
deserializer = JsonRowDeserializationSchema.builder().type_info(
    Types.ROW_NAMED(
        ["productId", "containerId", "shipId", "name", "category",
         "quantity", "weightKg", "status", "timestamp"],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
         Types.STRING(), Types.INT(), Types.DOUBLE(), Types.STRING(), Types.STRING()]
    )
).build()

kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers(KAFKA_BROKERS)
    .set_topics(KAFKA_TOPIC)
    .set_group_id("flink_validation_group")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(deserializer)
    .build()
)

stream = env.from_source(
    source=kafka_source,
    watermark_strategy=WatermarkStrategy.no_watermarks(),
    source_name="Kafka Source"
)

# ==============================
# VALIDATION + TRANSFORMATION LOGIC
# ==============================
class ValidateAndTransform(FlatMapFunction):
    def flat_map(self, row):
        """
        New-style FlatMap for modern PyFlink: accept one argument and yield results.
        Yields tuples of (status, json_string) where status is "valid" or "invalid".
        """
        try:
            product = {
                "productId": row[0],
                "containerId": row[1],
                "shipId": row[2],
                "name": row[3],
                "category": row[4],
                "quantity": row[5],
                "weightKg": row[6],
                "status": row[7],
                "timestamp": row[8],
            }

            errors = []

            # 1. Required fields
            for k, v in product.items():
                if v in (None, "", "null"):
                    errors.append(f"missing_field_{k}")

            # 2. Domain checks
            if product["category"] not in VALID_CATEGORIES:
                errors.append("invalid_category")
            if product["status"] not in VALID_STATUSES:
                errors.append("invalid_status")

            # 3. Ranges and types
            if not isinstance(product["quantity"], int) or not (1 <= product["quantity"] <= 10000):
                errors.append("invalid_quantity")
            if not isinstance(product["weightKg"], (int, float)) or not (0.5 <= product["weightKg"] <= 100000.0):
                errors.append("invalid_weight")

            # 4. Referential
            if not str(product["shipId"]).startswith("SHIP_"):
                errors.append("invalid_shipId")
            if not str(product["containerId"]).startswith("CONT_"):
                errors.append("invalid_containerId")

            # 5. Timestamp
            try:
                event_time = datetime.strptime(product["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
                now = datetime.utcnow()
                if event_time > now + timedelta(minutes=5):
                    errors.append("timestamp_in_future")
                if (now - event_time).days > 3650:
                    errors.append("timestamp_too_old")
            except Exception:
                errors.append("invalid_timestamp_format")
                event_time = datetime.utcnow()

            # 6. Business rules
            if product["category"] == "Automobile" and product["weightKg"] < 100:
                errors.append("auto_weight_too_low")
            if product["category"] == "Food" and product["status"] == "DELIVERED" and product["quantity"] < 5:
                errors.append("food_delivery_qty_too_low")

            if errors:
                dls_record = {
                    "error_reason": errors,
                    "raw_event": product,
                    "failed_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                }
                # yield one invalid tuple
                yield ("invalid", json.dumps(dls_record))
                return

            # Transformations
            product["name"] = product["name"].strip().title()
            product["totalWeightKg"] = round(product["quantity"] * product["weightKg"], 2)
            product["density_kg_per_unit"] = round(product["weightKg"] / product["quantity"], 3)
            product["processed_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            product["ingestion_system"] = "Flink_ContainerValidation"
            product["partition_category"] = product["category"].lower()
            product["partition_date"] = event_time.strftime("%Y-%m-%d")

            yield ("valid", json.dumps(product))

        except Exception as e:
            yield ("invalid", json.dumps({
                "error_reason": [f"exception: {str(e)}"],
                "raw_event": str(row),
                "failed_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            }))


validated_stream = stream.flat_map(
    ValidateAndTransform(),
    output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
)

valid_stream = validated_stream.filter(lambda x: x[0] == "valid").map(lambda x: x[1])
invalid_stream = validated_stream.filter(lambda x: x[0] == "invalid").map(lambda x: x[1])

# Print both streams
valid_sink = (
    FileSink
    .for_row_format(f"file://{VALID_FILE}", Encoder.simple_string_encoder())
    .with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("valid")
        .with_part_suffix(".json")
        .build()
    )
    .build()
)

invalid_sink = (
    FileSink
    .for_row_format(f"file://{INVALID_FILE}", Encoder.simple_string_encoder())
    .with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("invalid")
        .with_part_suffix(".json")
        .build()
    )
    .build()
)

valid_stream.sink_to(valid_sink)
invalid_stream.sink_to(invalid_sink)

# ==============================
# WRITE TO FILES LOCALLY
# ==============================

# ==============================
# EXECUTE JOB
# ==============================
env.execute("Flink 2.0 KafkaSource + Validation + DLS")
