import json
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, DeliveryGuarantee
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.state import ValueStateDescriptor


class TruckTripAggregator(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.trip_count = runtime_context.get_state(
            ValueStateDescriptor("tripCount", Types.INT())
        )
        self.total_duration = runtime_context.get_state(
            ValueStateDescriptor("totalDuration", Types.FLOAT())
        )
        self.last_status = runtime_context.get_state(
            ValueStateDescriptor("lastStatus", Types.STRING())
        )

    def process_element(self, value, ctx):
        data = json.loads(value)

        truck_id = data["truckId"]
        status = data.get("status")
        dep_raw = data.get("departureTime")
        arr_raw = data.get("arrivalTime")

        # Always update last known status
        self.last_status.update(status)

        # Only compute trip duration when both timestamps exist
        if dep_raw and arr_raw:
            try:
                dep = datetime.fromisoformat(dep_raw.replace("Z", "+00:00"))
                arr = datetime.fromisoformat(arr_raw.replace("Z", "+00:00"))
                duration_min = (arr - dep).total_seconds() / 60.0

                count = self.trip_count.value() or 0
                total = self.total_duration.value() or 0.0

                count += 1
                total += duration_min

                self.trip_count.update(count)
                self.total_duration.update(total)

            except Exception:
                # If parsing fails → ignore trip but still update status
                pass

        # Compute aggregates
        count = self.trip_count.value() or 0
        total = self.total_duration.value() or 0.0
        avg_duration = round(total / count, 1) if count > 0 else 0.0

        result = {
            "truckId": truck_id,
            "tripCount": str(count),
            "avgTripDurationMinutes": str(avg_duration),
            "lastEventStatus": status,
            "eventTime": data.get("timestamp")
        }

        yield json.dumps(result)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # --- Kafka Source ---
    source = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29094") \
        .set_topics("truckEvent") \
        .set_group_id("flink-truck-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "truck-events-source"
    )

    # --- Processing ---
    processed = (
        stream
        .key_by(lambda x: json.loads(x)["truckId"])
        .process(TruckTripAggregator())
        # FIX: Python returns bytes → convert to string before Kafka sink
        .map(lambda x: x.decode("utf-8") if isinstance(x, bytes) else x, Types.STRING())
    )

    # --- Kafka Sink ---
    sink = KafkaSink.builder() \
        .set_bootstrap_servers("broker:29094") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("truckAggregates")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .set_transactional_id_prefix("truck-agg") \
        .build()

    processed.sink_to(sink)

    env.execute("Truck Trip Aggregation Job")


if __name__ == "__main__":
    main()
