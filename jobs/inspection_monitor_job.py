import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types


class LatestInspectionState(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor(
            "last_inspection_state",
            Types.STRING()
        )
        self.last_state = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        event = json.loads(value)

        latest_record = {
            "containerId": event.get("containerId"),
            "lastInspectionResult": event.get("result"),
            "lastInspectorId": event.get("inspectorId"),
            "lastInspectionTime": event.get("timestamp")
        }

        # update state
        self.last_state.update(json.dumps(latest_record))

        # print clean output
        print("[LATEST STATE]:", latest_record)

        yield json.dumps(latest_record)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29094") \
        .set_topics("inspectionEvent") \
        .set_group_id("inspection_monitor_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "inspection_events_source"
    )

    result_stream = (
        stream
        .key_by(lambda x: json.loads(x)["containerId"])
        .process(LatestInspectionState())
    )

    result_stream.print()

    env.execute("Container Inspection Monitoring Job")


if __name__ == "__main__":
    main()
