import json
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types


SLA_THRESHOLD_MINUTES = 10  # SLA = 10 minutes


class SLAMonitor(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        # State to track the first event timestamp
        self.first_seen_state = runtime_context.get_state(
            ValueStateDescriptor("first_seen_time", Types.STRING())
        )

    def process_element(self, value, ctx):
        event = json.loads(value)

        container_id = event.get("containerId")
        result = event.get("result")
        event_time_str = event.get("timestamp")

        # Convert timestamp to datetime
        event_time = datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))

        # Load the stored first seen time
        first_seen_str = self.first_seen_state.value()
        
        if first_seen_str is None:
            # This is the first time we see this container
            self.first_seen_state.update(event_time_str)
            print(f"[ARRIVAL] Container {container_id} started inspection at {event_time_str}")
            return
        
        # Convert stored arrival time
        first_seen_time = datetime.fromisoformat(first_seen_str.replace("Z", "+00:00"))

        # Only check SLA when result becomes RELEASED
        if result == "RELEASED":
            diff_minutes = (event_time - first_seen_time).total_seconds() / 60

            if diff_minutes > SLA_THRESHOLD_MINUTES:
                print(f"⚠️ SLA VIOLATION → Container {container_id} took {diff_minutes:.1f} minutes (limit {SLA_THRESHOLD_MINUTES}m)")
            else:
                print(f"✅ Cleared within SLA → Container {container_id} took {diff_minutes:.1f} minutes")

            # Optional: reset to allow another cycle later
            self.first_seen_state.clear()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29094") \
        .set_topics("inspectionEvent") \
        .set_group_id("sla_monitor_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "inspection_event_source"
    )

    result_stream = (
        stream
        .key_by(lambda x: json.loads(x)["containerId"])
        .process(SLAMonitor())
    )

    result_stream.print()

    env.execute("Container Clearance SLA Monitoring Job")


if __name__ == "__main__":
    main()
