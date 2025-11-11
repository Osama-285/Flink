# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.common import Types, Duration
# from pyflink.datastream.functions import ProcessWindowFunction
# from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
# from pyflink.datastream.window import SlidingEventTimeWindows
# from pyflink.common.time import Time
# from datetime import datetime, timezone
# import json


# # ---------------- ENV + SOURCE ----------------
# env = StreamExecutionEnvironment.get_execution_environment()
# env.set_parallelism(2)

# source = (
#     KafkaSource.builder()
#     .set_bootstrap_servers("broker:29094")
#     .set_topics("containerEvent")
#     .set_group_id("yard_congestion")
#     .set_value_only_deserializer(SimpleStringSchema())
#     .set_starting_offsets(KafkaOffsetsInitializer.earliest())
#     .build()
# )

# raw_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaSource")


# # ---------------- PARSE JSON ----------------
# def parse_event(value):
#     data = json.loads(value)
#     ts = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
#     return (
#         data["location"],      # yard name
#         data["containerId"],
#         ts
#     )

# parsed = raw_stream.map(
#     parse_event,
#     output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP()])
# )


# # ---------------- WATERMARKS ----------------
# def to_millis(ts):
#     if isinstance(ts, datetime):
#         if ts.tzinfo is None:
#             ts = ts.replace(tzinfo=timezone.utc)
#         return int(ts.timestamp() * 1000)
#     return int(ts)

# class EventTimestampAssigner(TimestampAssigner):
#     def extract_timestamp(self, element, record_timestamp):
#         return to_millis(element[2])

# wm_strategy = (
#     WatermarkStrategy
#     .for_bounded_out_of_orderness(Duration.of_seconds(30))
#     .with_timestamp_assigner(EventTimestampAssigner())
# )

# events = parsed.assign_timestamps_and_watermarks(wm_strategy)


# # ---------------- WINDOW FUNCTION ----------------
# class CongestionDetector(ProcessWindowFunction):

#     def process(self, key, context, elements, out):  # ✅ correct signature
#         count = len(list(elements))
#         threshold = 10  # example congestion threshold
#         if count > threshold:
#             window_end = datetime.fromtimestamp(context.window().end / 1000, tz=timezone.utc)
#             msg = f"⚠️ ALERT: {key} congestion! {count} containers in 10-min window ending {window_end}"
#             out.collect(msg)  # ✅ emit result instead of print()


# # ---------------- APPLY SLIDING WINDOW ----------------
# alert_stream = (
#     events
#     .key_by(lambda e: e[0])  # key = location (yard)
#     .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
#     .process(CongestionDetector(), output_type=Types.STRING())
# )

# # print alerts
# alert_stream.print()

# env.execute("YardCongestionDetector")

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types, Duration
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common.time import Time
from datetime import datetime, timezone
import json


# ---------------- ENV + SOURCE ----------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("broker:29094")
    .set_topics("containerEvent")
    .set_group_id("yard_congestion")
    .set_value_only_deserializer(SimpleStringSchema())
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .build()
)

raw_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaSource")


# ---------------- PARSE JSON ----------------
def parse_event(value):
    data = json.loads(value)
    ts = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    return (
        data["location"],
        data["containerId"],
        ts
    )

parsed = raw_stream.map(
    parse_event,
    output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP()])
)


# ---------------- WATERMARKS ----------------
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


# ---------------- WINDOW FUNCTION ----------------
class CongestionDetector(ProcessWindowFunction):

    def process(self, key, context, elements):   # ✅ only 3 args
        count = len(list(elements))
        threshold = 10
        if count > threshold:
            window_end = datetime.fromtimestamp(context.window().end / 1000, tz=timezone.utc)
            msg = f"⚠️ ALERT: {key} congestion! {count} containers in 10-min window ending {window_end}"
            return [msg]   # ✅ return list of outputs
        return []


# ---------------- APPLY SLIDING WINDOW ----------------
alert_stream = (
    events
    .key_by(lambda e: e[0])
    .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
    .process(CongestionDetector(), output_type=Types.STRING())
)

alert_stream.print()

env.execute("YardCongestionDetector")
