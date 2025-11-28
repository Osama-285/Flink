from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types, Duration
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.state import ValueStateDescriptor
from datetime import datetime, timezone
import json

# ---------------- ENV + SOURCE ----------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("broker:29094")
    .set_topics("containerEvent")
    .set_group_id("ship_turnaround")
    .set_value_only_deserializer(SimpleStringSchema())
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .build()
)

raw_stream = env.from_source(
    source, WatermarkStrategy.no_watermarks(), "KafkaSource"
)

# ---------------- PARSE JSON ----------------
def parse_event(value):
    data = json.loads(value)
    ts = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    return (
        data["shipId"],        # key
        data["eventType"],     # LOAD / UNLOAD
        ts
    )

parsed = raw_stream.map(
    parse_event,
    output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP()])
)

# ---------------- TIMESTAMP ASSIGNER ----------------
def to_millis(ts):
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return int(ts.timestamp() * 1000)
    return int(ts)

class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        return to_millis(element[2])

wm_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(30))
    .with_timestamp_assigner(EventTimestampAssigner())
)

events = parsed.assign_timestamps_and_watermarks(wm_strategy)

# ---------------- TURNAROUND TRACKER ----------------
class ShipTurnaround(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        self.first_load = ctx.get_state(ValueStateDescriptor("first_load", Types.SQL_TIMESTAMP()))
        self.last_unload = ctx.get_state(ValueStateDescriptor("last_unload", Types.SQL_TIMESTAMP()))

    def process_element(self, value, ctx):
        ship_id, event_type, ts = value

        if event_type == "LOAD":
            if self.first_load.value() is None:
                self.first_load.update(ts)

        elif event_type == "UNLOAD":
            self.last_unload.update(ts)
            first = self.first_load.value()
            if first is not None:
                turnaround_hours = (ts - first).total_seconds() / 3600.0
                print(f"Ship {ship_id} turnaround: {turnaround_hours:.2f} hours")
                self.first_load.clear()
                self.last_unload.clear()

events.key_by(lambda e: e[0]).process(ShipTurnaround())
env.execute("ShipTurnaroundTime")
