from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, BroadcastProcessFunction
from pyflink.common.time import Duration
from pyflink.datastream.state import MapStateDescriptor
import json
from datetime import datetime


# ======================================================
# Utilities
# ======================================================
def parse_time(ts: str) -> int:
    return int(datetime.fromisoformat(ts).timestamp() * 1000)


# ======================================================
# Parse Flight Events
# ======================================================
class ParseFlightEvent(MapFunction):
    def map(self, value):
        event = json.loads(value)

        try:
            delay = int(event.get("delay_minutes", 0))
        except (TypeError, ValueError):
            delay = 0

        return (
            str(event["flight_id"]),
            str(event["origin"]),
            str(event["event_type"]),
            delay,
            parse_time(event["event_time"])
        )


# ======================================================
# Parse Weather Events
# ======================================================
class ParseWeatherEvent(MapFunction):
    def map(self, value):
        event = json.loads(value)
        return (
            str(event["airport"]),
            bool(event.get("storm", False)),
            parse_time(event["event_time"])
        )


# ======================================================
# Delay Propagation Engine (Flight ⨝ Weather)
# ======================================================
class DelayPropagationEngine(BroadcastProcessFunction):

    def __init__(self, weather_state_desc):
        self.weather_state_desc = weather_state_desc

    def process_element(self, flight, read_only_ctx):
        flight_id, origin, event_type, delay, event_time = flight

        weather_state = read_only_ctx.get_broadcast_state(self.weather_state_desc)
        storm = weather_state.get(origin) or False

        if event_type == "DELAYED" or storm:
            yield json.dumps({
                "flight_id": flight_id,
                "origin": origin,
                "delay_minutes": delay,
                "weather_storm": storm,
                "impact_type": "DELAY_PROPAGATED",
                "event_time": datetime.utcfromtimestamp(event_time / 1000).isoformat()
            })

    def process_broadcast_element(self, weather, ctx):
        airport, storm, event_time = weather
        ctx.get_broadcast_state(self.weather_state_desc).put(airport, storm)


# ======================================================
# Main
# ======================================================
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    watermark = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(lambda e, ts: e[-1])

    # ---------------- Kafka Sources ----------------
    flight_source = KafkaSource.builder() \
        .set_bootstrap_servers("broker2:29094") \
        .set_topics("flightEvents") \
        .set_group_id("flink-delay-engine") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    weather_source = KafkaSource.builder() \
        .set_bootstrap_servers("broker2:29094") \
        .set_topics("weatherEvents") \
        .set_group_id("flink-delay-engine") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    flight_stream = env.from_source(flight_source, watermark, "flight-source") \
        .map(ParseFlightEvent(), output_type=Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.STRING(),
            Types.LONG(), Types.LONG()
        ]))

    weather_stream = env.from_source(weather_source, watermark, "weather-source") \
        .map(ParseWeatherEvent(), output_type=Types.TUPLE([
            Types.STRING(), Types.BOOLEAN(), Types.LONG()
        ]))

    # ---------------- Broadcast State ----------------
    weather_state_desc = MapStateDescriptor(
        "weather_state",
        Types.STRING(),   # airport
        Types.BOOLEAN()   # storm
    )

    delay_stream = flight_stream \
        .connect(weather_stream.broadcast(weather_state_desc)) \
        .process(DelayPropagationEngine(weather_state_desc),
                 output_type=Types.STRING())

    # ---------------- Kafka Sink ----------------
    sink = KafkaSink.builder() \
        .set_bootstrap_servers("broker2:29094") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("delayImpactEvents")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    delay_stream.sink_to(sink)

    env.execute("Airline Delay Propagation Engine - Flink 2.0")


if __name__ == "__main__":
    main()
