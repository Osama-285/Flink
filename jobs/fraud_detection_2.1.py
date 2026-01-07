# from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
# from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
# from pyflink.datastream.formats.json import JsonRowDeserializationSchema
# from pyflink.common import Types, WatermarkStrategy, Duration
# from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor, StateTtlConfig
# from pyflink.datastream.checkpointing_mode import CheckpointingMode
# from datetime import datetime
# import math

# MAX_AMOUNT = 200
# RAPID_WINDOW_SECONDS = 10
# RAPID_TX_COUNT = 3
# IMPOSSIBLE_TRAVEL_SECONDS = 120
# EARTH_RADIUS_KM = 6371

# LOCATION_COORDS = {
#     "NY": (40.7128, -74.0060),
#     "CA": (34.0522, -118.2437),
#     "TX": (29.7604, -95.3698),
#     "FL": (25.7617, -80.1918),
#     "IL": (41.8781, -87.6298)
# }


# def haversine(loc1, loc2):
#     lat1, lon1 = loc1
#     lat2, lon2 = loc2

#     dlat = math.radians(lat2 - lat1)
#     dlon = math.radians(lon2 - lon1)

#     a = (
#         math.sin(dlat / 2) ** 2 +
#         math.cos(math.radians(lat1)) *
#         math.cos(math.radians(lat2)) *
#         math.sin(dlon / 2) ** 2
#     )
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
#     return EARTH_RADIUS_KM * c

# class FraudDetector(KeyedProcessFunction):


#     def open(self, runtime_context):
#         TTL_CONFIG = (
#             StateTtlConfig
#             .new_builder(Duration.of_minutes(10))
#             .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
#             .cleanup_full_snapshot()
#             .build()
#         )

#         tx_history_desc = ListStateDescriptor(
#             "tx_history",
#             Types.TUPLE([Types.LONG(), Types.FLOAT()])
#         )

#         tx_history_desc.enable_time_to_live(TTL_CONFIG)
#         self.tx_history = runtime_context.get_list_state(tx_history_desc)

#         last_location_desc = ValueStateDescriptor(
#             "last_location",
#             Types.STRING()
#         )
#         last_location_desc.enable_time_to_live(TTL_CONFIG)
#         self.last_location = runtime_context.get_state(last_location_desc)

#         last_event_time_desc = ValueStateDescriptor(
#             "last_event_time",
#             Types.LONG()
#         )
#         last_event_time_desc.enable_time_to_live(TTL_CONFIG)
#         self.last_event_time = runtime_context.get_state(last_event_time_desc)

#     def process_element(self, event, ctx):
#         alerts = []
#         event_time = int(datetime.fromisoformat(event.timestamp.replace("Z","")).timestamp())

#         amount = float(event.amount)
#         location = event.location
#         card_id = event.card_id

#         if amount > MAX_AMOUNT:
#             alerts.append({
#                 "rule": "HIGH_AMOUNT",
#                 "severity": "HIGH",
#                 "card_id": card_id,
#                 "amount": amount,
#                 "event_time": event.timestamp
#             })

#         history = list(self.tx_history.get())
#         history.append((event_time, amount))

#         recent_tx = [
#             ts for ts, _ in history
#             if event_time - ts <= RAPID_WINDOW_SECONDS
#         ]

#         self.tx_history.update(history)

#         if len(recent_tx) >= RAPID_TX_COUNT:
#             alerts.append({
#                 "rule": "VELOCITY",
#                 "severity": "MEDIUM",
#                 "card_id": card_id,
#                 "tx_count": len(recent_tx),
#                 "window_sec": RAPID_WINDOW_SECONDS,
#                 "event_time": event.timestamp
#             })
#         last_loc = self.last_location.value()
#         last_time = self.last_event_time.value()

#         if last_loc and last_time:
#             if(location != last_loc and
#                 event_time - last_time <= IMPOSSIBLE_TRAVEL_SECONDS):

#                 dist = haversine(
#                     LOCATION_COORDS[last_loc],
#                     LOCATION_COORDS[location]
#                 )
#                 if dist > 500:
#                     alerts.append({
#                         "rule": "IMPOSSIBLE_TRAVEL",
#                         "severity": "HIGH",
#                         "card_id": card_id,
#                         "from": last_loc,
#                         "to": location,
#                         "distance_km": round(dist, 2),
#                         "event_time": event.timestamp
#                     })

#         self.last_location.update(location)
#         self.last_event_time.update(event_time)

#         for alert in alerts:
#             yield alert


# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(4)

#     env.enable_checkpointing(60000)
#     env.get_checkpoint_config().set_checkpointing_mode(
#         CheckpointingMode.EXACTLY_ONCE
#     )

#     kafka_source = (
#         KafkaSource.builder()
#         .set_bootstrap_servers("broker2:29094")
#         .set_topics("transactions")
#         .set_group_id("fraud-detector-V1")
#         .set_starting_offsets(KafkaOffsetsInitializer.earliest())
#         .set_value_only_deserializer(
#             JsonRowDeserializationSchema.builder().type_info(
#                 Types.ROW_NAMED([
#                     "transaction_id",
#                     "customer_id",
#                     "card_id",
#                     "merchant_id",
#                     "merchant_category",
#                     "amount",
#                     "currency",
#                     "location",
#                     "ip_address",
#                     "event_type",
#                     "timestamp"
#                 ],
#                 [
#                     Types.STRING(),
#                     Types.STRING(),
#                     Types.STRING(),
#                     Types.STRING(),
#                     Types.STRING(),
#                     Types.FLOAT(),
#                     Types.STRING(),
#                     Types.STRING(),
#                     Types.STRING(),
#                     Types.STRING(),
#                     Types.STRING()
#                 ])
#             ).build()
#         ).build()
#     )

#     watermark_stratergy = (
#         WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)).with_timestamp_assigner(
#             lambda e, ts: int(
#                 datetime.fromisoformat(
#                     e.timestamp.replace("Z","")
#                 ).timestamp() * 1000
#             )
#         )
#     )

#     stream = env.from_source(
#         kafka_source,
#         watermark_stratergy,
#         "Kafka Transactions Source"
#     )

#     alerts = (
#         stream.key_by(lambda e: e.card_id)
#         .process(FraudDetector())
#     )

#     alerts.print()

#     env.execute("real-time-fraud-detection")

# if __name__ == "__main__":
#     main()

# from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, OutputTag
# from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
# from pyflink.datastream.formats.json import JsonRowDeserializationSchema
# from pyflink.common import Types, WatermarkStrategy, Duration
# from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor, StateTtlConfig
# from pyflink.datastream.checkpointing_mode import CheckpointingMode
# from datetime import datetime
# import math

# # ----------------------------- CONFIGURATION -----------------------------
# MAX_AMOUNT = 200
# RAPID_WINDOW_SECONDS = 10
# RAPID_TX_COUNT = 3
# IMPOSSIBLE_TRAVEL_SECONDS = 120
# EARTH_RADIUS_KM = 6371

# LOCATION_COORDS = {
#     "NY": (40.7128, -74.0060),
#     "CA": (34.0522, -118.2437),
#     "TX": (29.7604, -95.3698),
#     "FL": (25.7617, -80.1918),
#     "IL": (41.8781, -87.6298)
# }

# # TTL CONFIG created at module level to avoid PythonGatewayServer errors


# ALERT_TAG = OutputTag("alerts", Types.MAP(Types.STRING(), Types.STRING()))

# # ----------------------------- HELPER FUNCTIONS -----------------------------
# def haversine(loc1, loc2):
#     lat1, lon1 = loc1
#     lat2, lon2 = loc2
#     dlat = math.radians(lat2 - lat1)
#     dlon = math.radians(lon2 - lon1)
#     a = (
#         math.sin(dlat / 2) ** 2 +
#         math.cos(math.radians(lat1)) *
#         math.cos(math.radians(lat2)) *
#         math.sin(dlon / 2) ** 2
#     )
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
#     return EARTH_RADIUS_KM * c

# # ----------------------------- FRAUD DETECTOR -----------------------------
# class FraudDetector(KeyedProcessFunction):

#     def open(self, runtime_context):
#         # Transaction history state
#         TTL_CONFIG = (
#             StateTtlConfig
#             .new_builder(Duration.of_minutes(10))
#             .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
#             .cleanup_full_snapshot()
#             .build()
#         )
#         tx_history_desc = ListStateDescriptor(
#             "tx_history",
#             Types.TUPLE([Types.LONG(), Types.FLOAT()])
#         )
#         tx_history_desc.enable_time_to_live(TTL_CONFIG)
#         self.tx_history = runtime_context.get_list_state(tx_history_desc)

#         # Last location state
#         last_location_desc = ValueStateDescriptor(
#             "last_location",
#             Types.STRING()
#         )
#         last_location_desc.enable_time_to_live(TTL_CONFIG)
#         self.last_location = runtime_context.get_state(last_location_desc)

#         # Last event timestamp state
#         last_event_time_desc = ValueStateDescriptor(
#             "last_event_time",
#             Types.LONG()
#         )
#         last_event_time_desc.enable_time_to_live(TTL_CONFIG)
#         self.last_event_time = runtime_context.get_state(last_event_time_desc)

#     def process_element(self, event, ctx):
#         alerts = []
#         event_time = int(datetime.fromisoformat(event.timestamp.replace("Z","")).timestamp() * 1000)
#         amount = float(event.amount)
#         location = event.location
#         card_id = event.card_id

#         # --- HIGH AMOUNT ---
#         if amount > MAX_AMOUNT:
#             alerts.append({
#                 "rule": "HIGH_AMOUNT",
#                 "severity": "HIGH",
#                 "card_id": card_id,
#                 "amount": str(amount),
#                 "event_time": event.timestamp
#             })

#         # --- VELOCITY ---
#         history = list(self.tx_history.get())
#         history.append((event_time, amount))
#         recent_tx = [ts for ts, _ in history if event_time - ts <= RAPID_WINDOW_SECONDS]
#         self.tx_history.update(history)

#         if len(recent_tx) >= RAPID_TX_COUNT:
#             alerts.append({
#                 "rule": "VELOCITY",
#                 "severity": "MEDIUM",
#                 "card_id": card_id,
#                 "tx_count": str(len(recent_tx)),
#                 "window_sec": str(RAPID_WINDOW_SECONDS),
#                 "event_time": event.timestamp
#             })

#         # --- IMPOSSIBLE TRAVEL ---
#         last_loc = self.last_location.value()
#         last_time = self.last_event_time.value()

#         if last_loc and last_time:
#             if location != last_loc and event_time - last_time <= IMPOSSIBLE_TRAVEL_SECONDS:
#                 dist = haversine(LOCATION_COORDS[last_loc], LOCATION_COORDS[location])
#                 if dist > 500:  # 500km threshold
#                     alerts.append({
#                         "rule": "IMPOSSIBLE_TRAVEL",
#                         "severity": "HIGH",
#                         "card_id": card_id,
#                         "from": last_loc,
#                         "to": location,
#                         "distance_km": str(round(dist, 2)),
#                         "event_time": event.timestamp
#                     })

#         self.last_location.update(location)
#         self.last_event_time.update(event_time)

#         # Send alerts to side output
#         for alert in alerts:
#             ctx.output(ALERT_TAG, alert)

# # ----------------------------- MAIN EXECUTION -----------------------------
# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(4)

#     # Checkpointing for production-grade resilience
#     env.enable_checkpointing(60000)
#     env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
#     env.get_checkpoint_config().set_min_pause_between_checkpoints(500)
#     env.get_checkpoint_config().set_checkpoint_timeout(60000)

#     kafka_source = (
#         KafkaSource.builder()
#         .set_bootstrap_servers("broker2:29094")
#         .set_topics("transactions")
#         .set_group_id("fraud-detector-V2")
#         .set_starting_offsets(KafkaOffsetsInitializer.earliest())
#         .set_value_only_deserializer(
#             JsonRowDeserializationSchema.builder().type_info(
#                 Types.ROW_NAMED(
#                     ["transaction_id","customer_id","card_id","merchant_id","merchant_category",
#                      "amount","currency","location","ip_address","event_type","timestamp"],
#                     [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
#                      Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING(),
#                      Types.STRING(), Types.STRING(), Types.STRING()]
#                 )
#             ).build()
#         ).build()
#     )

#     watermark_strategy = (
#         WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
#         .with_timestamp_assigner(lambda e, ts: int(datetime.fromisoformat(e.timestamp.replace("Z","")).timestamp() * 1000))
#     )

#     stream = env.from_source(
#         kafka_source,
#         watermark_strategy,
#         "Kafka Transactions Source"
#     )

#     alerts = (
#         stream.key_by(lambda e: e.card_id)
#         .process(FraudDetector())
#     )

#     # Print side-output alerts
#     alerts.get_side_output(ALERT_TAG).print()

#     env.execute("real-time-fraud-detection")

# if __name__ == "__main__":
#     main()

from pyflink.datastream import (
    StreamExecutionEnvironment,
    KeyedProcessFunction,
    OutputTag,
)
from pyflink.common.time import Time
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.datastream.state import (
    ListStateDescriptor,
    ValueStateDescriptor,
    StateTtlConfig,
)
from pyflink.datastream.functions import RuntimeContext
from datetime import datetime
import math
import json

MAX_AMOUNT = 200
RAPID_WINDOW_MS = 10_000
RAPID_TX_COUNT = 3
IMPOSSIBLE_TRAVEL_MS = 120_000
EARTH_RADIUS_KM = 6371

LOCATION_COORDS = {
    "NY": (40.7128, -74.0060),
    "CA": (34.0522, -118.2437),
    "TX": (29.7604, -95.3698),
    "FL": (25.7617, -80.1918),
    "IL": (41.8781, -87.6298),
}

FRAUD_TAG = OutputTag("fraud", Types.STRING())
ALERT_TAG = OutputTag("alerts", Types.MAP(Types.STRING(), Types.STRING()))


def haversine(a, b):
    lat1, lon1 = a
    lat2, lon2 = b
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    x = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.atan2(math.sqrt(x), math.sqrt(1 - x))


class FraudDetector(KeyedProcessFunction):

    def open(self, ctx:RuntimeContext):
        ttl = (
            StateTtlConfig
            .new_builder(Time.minutes(10))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .build()
        )

        tx_desc = ListStateDescriptor(
            "txs",
            Types.TUPLE([Types.LONG(), Types.FLOAT()])
        )
        tx_desc.enable_time_to_live(ttl)
        self.tx_state = ctx.get_list_state(tx_desc)

        loc_desc = ValueStateDescriptor("loc", Types.STRING())
        loc_desc.enable_time_to_live(ttl)
        self.last_loc = ctx.get_state(loc_desc)

        time_desc = ValueStateDescriptor("time", Types.LONG())
        time_desc.enable_time_to_live(ttl)
        self.last_time = ctx.get_state(time_desc)
    def process_element(self, value, ctx):
        score = 0

        card_id = value[2]
        amount = float(value[5])
        location = value[7]
        ts = value[10]

        event_time = int(
            datetime.fromisoformat(ts.replace("Z", "")).timestamp() * 1000
        )

        if amount > MAX_AMOUNT:
            score += 40

        history = list(self.tx_state.get())
        history.append((event_time, amount))
        self.tx_state.update(history)

        recent = [t for t, _ in history if event_time - t <= RAPID_WINDOW_MS]
        if len(recent) >= RAPID_TX_COUNT:
            score += 30

        last_loc = self.last_loc.value()
        last_time = self.last_time.value()

        if (
            last_loc and last_time
            and location != last_loc
            and event_time - last_time <= IMPOSSIBLE_TRAVEL_MS
            and location in LOCATION_COORDS
            and last_loc in LOCATION_COORDS
        ):
            dist = haversine(
                LOCATION_COORDS[last_loc],
                LOCATION_COORDS[location]
            )
            if dist > 500:
                score += 50

        result = {
            "card_id": card_id,
            "amount": amount,
            "location": location,
            "score": score,
            "status": "FRAUD" if score >= 40 else "LEGIT",
            "severity": "HIGH" if score >= 70 else "NONE",
            "event_time": ts,
        }

        self.last_loc.update(location)
        self.last_time.update(event_time)

        yield json.dumps(result)

# --------------- MAIN -------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    kafka = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker2:29094")
        .set_topics("transactions")
        .set_group_id("fraud-v3")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder()
            .type_info(
                Types.ROW_NAMED(
                    [
                        "transaction_id",
                        "customer_id",
                        "card_id",
                        "merchant_id",
                        "merchant_category",
                        "amount",
                        "currency",
                        "location",
                        "ip_address",
                        "event_type",
                        "timestamp",
                    ],
                    [
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.FLOAT(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                    ],
                )
            )
            .build()
        )
        .build()
    )

    wm = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    ).with_timestamp_assigner(
        lambda e, ts: int(
            datetime.fromisoformat(e[10].replace("Z", "")).timestamp() * 1000
        )
    )

    stream = env.from_source(kafka, wm, "kafka-source")

    # stream.key_by(lambda e: e[2]).process(FraudDetector()).get_side_output(
    #     ALERT_TAG
    # ).print()

    processed = (
        stream
            .key_by(lambda e: e[2])
            .process(
                FraudDetector(),
                output_type=Types.STRING()
            )
    )

    fraud = (
        processed
            .filter(lambda x: json.loads(x)["status"] == "FRAUD")
            .map(lambda x: f"FRAUD | {x}")
    )

    legit = (
        processed
            .filter(lambda x: json.loads(x)["status"] == "LEGIT")
            .map(lambda x: f"LEGIT | {x}")
    )

    fraud.print()
    legit.print()

  

    env.execute("real-time-fraud-detection")


if __name__ == "__main__":
    main()
