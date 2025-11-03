import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema    
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    DeliveryGuarantee, KafkaOffsetsInitializer
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.output_tag import OutputTag

# ----------------------------
# VALIDATION CONSTANTS
# ----------------------------
VALID_CATEGORIES = {
    "Food": ["Rice", "Wheat", "Bananas", "Apples", "Oranges"],
    "Automobile": ["Car", "Truck", "Motorbike", "SpareParts"],
    "Electronics": ["TV", "Laptop", "Smartphone", "Tablet", "Router"],
    "Furniture": ["Chair", "Table", "Sofa", "Bed", "Desk"],
    "Clothing": ["Shirt", "Jeans", "Shoes", "Jacket"]
}

VALID_STATUSES = ["IN_CONTAINER", "UNLOADED", "DELIVERED"]


# ----------------------------
# PRODUCT VALIDATION LOGIC
# ----------------------------
class ProductValidator(ProcessFunction):
    def __init__(self, invalid_output_tag):
        self.invalid_output_tag = invalid_output_tag

    def process_element(self, value, ctx):
        if value is None:
            ctx.output(self.invalid_output_tag, {"error": "Invalid JSON", "raw": str(value)})
            return

        errors = []
        category = value.get("category")
        name = value.get("name")
        quantity = value.get("quantity")
        weight = value.get("weightKg")
        status = value.get("status")

        # --- Validation rules ---
        if category not in VALID_CATEGORIES:
            errors.append(f"Invalid category: {category}")
        elif name not in VALID_CATEGORIES[category]:
            errors.append(f"Invalid product name '{name}' for category '{category}'")

        if not isinstance(quantity, int) or quantity <= 0:
            errors.append(f"Invalid quantity: {quantity}")

        if not isinstance(weight, (int, float)) or weight <= 0:
            errors.append(f"Invalid weight: {weight}")

        if status not in VALID_STATUSES:
            errors.append(f"Invalid status: {status}")

        if errors:
            ctx.output(self.invalid_output_tag, {"errors": errors, "event": value})
        else:
            yield value


# ----------------------------
# MAIN EXECUTION
# ----------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker:29094")
        .set_topics("productEvent")
        .set_group_id("flink-validator")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())  
        .build()
    )

    valid_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("broker:29094")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("validated_products")
            .set_value_serialization_schema(SimpleStringSchema())  
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_transactional_id_prefix("valid-products-txn")
        .build()
    )

    invalid_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("broker:29094")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("invalid_products")
            .set_value_serialization_schema(SimpleStringSchema())  
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_transactional_id_prefix("invalid-products-txn")
        .build()
    )


    invalid_tag = OutputTag("invalid", Types.MAP(Types.STRING(), Types.STRING()))

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    parsed_stream = ds.map(lambda x: json.loads(x), output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()))

    validated = parsed_stream.process(ProductValidator(invalid_tag))
    invalid = validated.get_side_output(invalid_tag)

    validated.map(lambda e: f"[VALID] {e}").print()
    invalid.map(lambda e: f"[INVALID] {e}").print()

    validated.map(lambda e: json.dumps(e), output_type=Types.STRING()).sink_to(valid_sink)
    invalid.map(lambda e: json.dumps(e), output_type=Types.STRING()).sink_to(invalid_sink)

    env.execute("Product Event Validation Job")


if __name__ == "__main__":
    main()
