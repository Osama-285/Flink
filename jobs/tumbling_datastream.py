import json
import datetime

from pyflink.common import Duration, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import KeySelector, ProcessWindowFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time
# from pyflink.datastream.window import TimeWindow
# from typing import Iterable


# -------- Helpers --------

def parse_json(value: str) -> Row:
    """Turn Kafka JSON string into a Row with epoch millis for Flink."""
    rec = json.loads(value)

    # convert ISO8601 -> epoch millis
    ts = datetime.datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00"))
    ts_millis = int(ts.timestamp() * 1000)

    return Row(
        flight=rec["flight"],
        airline=rec["airline"],
        altitude=int(rec["altitude"]),
        speed=int(rec["speed"]),
        status=rec["status"],
        event_time=ts_millis,  # store epoch
        flight_count=1
    )


class AirlineKeySelector(KeySelector):
    def get_key(self, value: Row):
        return value.airline


from pyflink.common.watermark_strategy import TimestampAssigner


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value.event_time


# A ProcessWindowFunction to emit (airline, count, window_start, window_end)
class CountWindow(ProcessWindowFunction):
    def process(self, key, context, elements):
        total = sum(e.flight_count for e in elements)
        yield Row(
            airline=key,
            window_start=context.window().start,
            window_end=context.window().end,
            flight_count=total
        )



# -------- Main --------

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # --- Kafka Source ---
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker:29094")
        .set_topics("flight")
        .set_group_id("flight-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="KafkaFlightSource"
    )

    row_type = Types.ROW_NAMED(
        ["flight", "airline", "altitude", "speed", "status", "event_time", "flight_count"],
        [Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.LONG(), Types.INT()]
    )

    parsed = raw.map(parse_json, output_type=row_type)

    # Watermark Strategy
    wm = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(EventTimeAssigner())
    )
    with_wm = parsed.assign_timestamps_and_watermarks(wm)

    # Output row type for the aggregated result
    out_type = Types.ROW_NAMED(
        ["airline", "window_start", "window_end", "flight_count"],
        [Types.STRING(), Types.LONG(), Types.LONG(), Types.INT()]
    )

    windowed = (
        with_wm
        .key_by(AirlineKeySelector(), key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .process(
            CountWindow(),
            output_type=Types.ROW_NAMED(
                ["airline", "window_start", "window_end", "flight_count"],
                [Types.STRING(), Types.LONG(), Types.LONG(), Types.INT()]
            )
        )
    )



    windowed.print()
    env.execute("FlightTumblingCount")


if __name__ == "__main__":
    main()
