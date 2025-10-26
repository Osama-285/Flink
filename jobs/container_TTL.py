from pyflink.datastream.state import ValueStateDescriptor
import json
from datetime import datetime, timedelta, timezone
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types, Duration
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)
source = (
    KafkaSource.builder()
    .set_bootstrap_servers("broker:29094")
    .set_topics("containerEvent")
    .set_group_id("delay_detector")
    .set_value_only_deserializer(SimpleStringSchema())
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .build()
)

raw_stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "KafkaSource"
)


# -------------------------------------------------------------------------
# 3️⃣ Parse JSON
# -------------------------------------------------------------------------
def parse_event(value: str):
    data = json.loads(value)
    ts = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    return (
        data["containerId"],
        data["eventType"],
        ts
    )

parsed = raw_stream.map(parse_event,
    output_type=Types.TUPLE([
        Types.STRING(),      # containerId
        Types.STRING(),      # eventType
        Types.SQL_TIMESTAMP()  # event time
    ])
)

def to_millis(ts):
    """Convert to epoch milliseconds (handles Python + Java timestamp)."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return int(ts.timestamp() * 1000)
    return int((datetime.utcnow() - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds() * 1000)

class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        return to_millis(element[2])

wm_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(30))
    .with_timestamp_assigner(EventTimestampAssigner())
)

events = parsed.assign_timestamps_and_watermarks(wm_strategy)

class DelayDetector(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        descriptor = ValueStateDescriptor(
            "last_load", Types.SQL_TIMESTAMP()
        )
        self.last_load = ctx.get_state(descriptor)

    def process_element(self, value, ctx):
        cid, event_type, ts = value

        if event_type == "LOAD":
            self.last_load.update(ts)
            trigger_time = to_millis(ts) + 24 * 3600 * 1000
            ctx.timer_service().register_event_time_timer(trigger_time)

        elif event_type == "DELIVERED":
            self.last_load.clear()

    def on_timer(self, timestamp, ctx):
        cid = ctx.get_current_key()
        last = self.last_load.value()
        if last is not None:
            print(f"⚠️ ALERT: Container {cid} delayed > 24h since LOAD at {last}")
            self.last_load.clear()

events.key_by(lambda e: e[0]).process(DelayDetector())

env.execute("AdvancedContainerDelayDetection")
