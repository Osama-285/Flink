# """
# multi_topic_container_analytics.py

# PyFlink job that:
#  - Consumes productEvent, truckEvent, staffEvent, inspectionEvent (each is its own Kafka topic)
#  - Uses event-time with bounded out-of-orderness watermarks
#  - Computes:
#     * 5-minute throughput per warehouse (product events)
#     * Dwell time per container (truck departure -> arrival) (stateful)
#     * Daily inspection failure rate (inspection events)
#     * Staff shift durations (stateful)
#  - Emits alerts (side output) for flagged inspections and missing arrivals
#  - Writes aggregates to analytics_agg and alerts to analytics_alerts Kafka topics
# """

# import json
# import logging
# from datetime import datetime, timedelta
# from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
# from pyflink.common import Types, Time, Duration
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
# from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction, RuntimeContext
# from pyflink.datastream.state import ValueStateDescriptor
# from pyflink.datastream import OutputTag

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("multi-topic-analytics")

# # ---------------- CONFIG ----------------
# KAFKA_BOOTSTRAP = "broker:29094"  
# GROUP_ID = "flink-multi-topic-analytics-v1"

# TOPIC_PRODUCT = "productEvent"
# TOPIC_TRUCK = "truckEvent"
# TOPIC_STAFF = "staffEvent"
# TOPIC_INSPECTION = "inspectionEvent"

# OUT_TOPIC_AGG = "analytics_agg"
# OUT_TOPIC_ALERTS = "analytics_alerts"

# # how late events are tolerated (tweak to your data)
# MAX_OUT_OF_ORDERNESS_SECONDS = 30

# # If departure seen but no arrival within this many hours -> emit missing-arrival alert
# MISSING_ARRIVAL_HOURS = 6

# # ----------------- Helpers -----------------
# def parse_json_safe(s: str):
#     try:
#         return json.loads(s)
#     except Exception:
#         logger.warning("Failed to parse JSON: %s", s)
#         return None

# def iso_to_epoch_ms(iso_ts: str):
#     """
#     Convert ISO 8601 string like "2025-11-04T12:34:56Z" to epoch ms.
#     Falls back to current time if parse fails.
#     """
#     if not iso_ts:
#         return int(datetime.utcnow().timestamp() * 1000)
#     try:
#         # Accepts "YYYY-MM-DDTHH:MM:SSZ"
#         return int(datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%SZ").timestamp() * 1000)
#     except Exception:
#         # Try a few flexible parses if necessary
#         try:
#             return int(datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).timestamp() * 1000)
#         except Exception:
#             logger.warning("timestamp parse failed for %s", iso_ts)
#             return int(datetime.utcnow().timestamp() * 1000)

# def to_iso_from_epoch_ms(ms: int):
#     return datetime.utcfromtimestamp(ms/1000).strftime("%Y-%m-%dT%H:%M:%SZ")

# # ---------------- Execution ----------------
# def create_kafka_source(topic: str, bootstrap: str, group_id: str):
#     return KafkaSource.builder() \
#         .set_bootstrap_servers(bootstrap) \
#         .set_group_id(group_id) \
#         .set_topics(topic) \
#         .set_value_only_deserializer(SimpleStringSchema()) \
#         .build()

# def create_kafka_sink(topic: str, bootstrap: str):
#     return KafkaSink.builder() \
#         .set_bootstrap_servers(bootstrap) \
#         .set_record_serializer(
#             KafkaRecordSerializationSchema.builder()
#             .set_topic(topic)
#             .set_value_serialization_schema(SimpleStringSchema())
#             .build()
#         ).build()

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(4)
#     # optional: env.set_stream_time_characteristic(TimeCharacteristic.EventTime)  # older versions
#     env.get_config().set_auto_watermark_interval(1000)

#     # ---------------- Watermark Strategy (common) ----------------
#     wm_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(MAX_OUT_OF_ORDERNESS_SECONDS)) \
#         .with_timestamp_assigner(lambda ev, ts: iso_to_epoch_ms(ev.get("timestamp")) if isinstance(ev, dict) and ev.get("timestamp") else ts)

#     # ---------------- Sources ----------------
#     prod_src = create_kafka_source(TOPIC_PRODUCT, KAFKA_BOOTSTRAP, GROUP_ID)
#     truck_src = create_kafka_source(TOPIC_TRUCK, KAFKA_BOOTSTRAP, GROUP_ID)
#     staff_src = create_kafka_source(TOPIC_STAFF, KAFKA_BOOTSTRAP, GROUP_ID)
#     insp_src = create_kafka_source(TOPIC_INSPECTION, KAFKA_BOOTSTRAP, GROUP_ID)

#     raw_prod = env.from_source(prod_src, wm_strategy, "product-source", type_info=Types.STRING())
#     raw_truck = env.from_source(truck_src, wm_strategy, "truck-source", type_info=Types.STRING())
#     raw_staff = env.from_source(staff_src, wm_strategy, "staff-source", type_info=Types.STRING())
#     raw_insp = env.from_source(insp_src, wm_strategy, "inspection-source", type_info=Types.STRING())

#     # ---------------- Parse JSON ----------------
#     prod_parsed = raw_prod.map(lambda s: parse_json_safe(s), output_type=Types.MAP(Types.STRING(), Types.STRING())).filter(lambda x: x is not None)
#     truck_parsed = raw_truck.map(lambda s: parse_json_safe(s), output_type=Types.MAP(Types.STRING(), Types.STRING())).filter(lambda x: x is not None)
#     staff_parsed = raw_staff.map(lambda s: parse_json_safe(s), output_type=Types.MAP(Types.STRING(), Types.STRING())).filter(lambda x: x is not None)
#     insp_parsed = raw_insp.map(lambda s: parse_json_safe(s), output_type=Types.MAP(Types.STRING(), Types.STRING())).filter(lambda x: x is not None)

#     # Re-assign timestamp assigner per parsed dict (safer)
#     prod_parsed = prod_parsed.assign_timestamps_and_watermarks(
#         WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(MAX_OUT_OF_ORDERNESS_SECONDS))
#         .with_timestamp_assigner(lambda ev, ts: iso_to_epoch_ms(ev.get("timestamp")))
#     )
#     truck_parsed = truck_parsed.assign_timestamps_and_watermarks(
#         WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(MAX_OUT_OF_ORDERNESS_SECONDS))
#         .with_timestamp_assigner(lambda ev, ts: iso_to_epoch_ms(ev.get("timestamp") or ev.get("departureTime") or ev.get("arrivalTime")))
#     )
#     staff_parsed = staff_parsed.assign_timestamps_and_watermarks(
#         WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(MAX_OUT_OF_ORDERNESS_SECONDS))
#         .with_timestamp_assigner(lambda ev, ts: iso_to_epoch_ms(ev.get("timestamp")))
#     )
#     insp_parsed = insp_parsed.assign_timestamps_and_watermarks(
#         WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(MAX_OUT_OF_ORDERNESS_SECONDS))
#         .with_timestamp_assigner(lambda ev, ts: iso_to_epoch_ms(ev.get("timestamp")))
#     )

#     # ---------------- Analytics 1: 5-min throughput per warehouse (product events) ----------------
#     # map -> (destination, 1, ts)
#     prod_kv = prod_parsed.map(lambda ev: (ev.get("destination") or "UNK", 1, iso_to_epoch_ms(ev.get("timestamp"))),
#                                output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()]))

#     from pyflink.datastream.window import TumblingEventTimeWindows
#     prod_by_wh = prod_kv.key_by(lambda t: t[0], key_type=Types.STRING())
#     prod_windowed = prod_by_wh.window(TumblingEventTimeWindows.of(Time.minutes(5))) \
#         .reduce(lambda a, b: (a[0], a[1] + b[1], max(a[2], b[2])),
#                 output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()]))

#     def prod_agg_to_json(rec):
#         warehouse, cnt, ts = rec
#         return json.dumps({
#             "metric": "throughput",
#             "warehouse": warehouse,
#             "count": cnt,
#             "window_end_ts": to_iso_from_epoch_ms(ts)
#         })

#     prod_agg_json = prod_windowed.map(prod_agg_to_json, output_type=Types.STRING())

#     # ---------------- Analytics 2: Dwell time per container (truck events: departure -> arrival) ----------------
#     alert_tag = OutputTag("alerts", Types.STRING())

#     class TruckDwellProcess(KeyedProcessFunction):
#         """
#         key: containerId
#         Behavior:
#           - on departureTime: store departure_ms in state, register timer at departure_ms + MISSING_ARRIVAL_HOURS
#           - on arrivalTime: if departure exists -> compute dwell and emit; clear state and delete timer
#           - timer callback: if fired and arrival not seen -> emit missing-arrival alert and clear state
#         """
#         def open(self, runtime_context: RuntimeContext):
#             self.departure_state = runtime_context.get_state(ValueStateDescriptor("departure", Types.LONG()))
#             self.timer_state = runtime_context.get_state(ValueStateDescriptor("timer_ts", Types.LONG()))

#         def process_element(self, ev, ctx: 'KeyedProcessFunction.Context'):
#             # ev is a map
#             dep = ev.get("departureTime")
#             arr = ev.get("arrivalTime")
#             container = ev.get("containerId") or "UNK"

#             if dep:
#                 dep_ms = iso_to_epoch_ms(dep)
#                 self.departure_state.update(dep_ms)
#                 # register event-time timer for missing arrival
#                 timer_ms = dep_ms + int(MISSING_ARRIVAL_HOURS * 3600 * 1000)
#                 prev = self.timer_state.value()
#                 # register only if not present or new timer is later
#                 ctx.timer_service().register_event_time_timer(timer_ms)
#                 self.timer_state.update(timer_ms)
#             if arr:
#                 arr_ms = iso_to_epoch_ms(arr)
#                 ld = self.departure_state.value()
#                 if ld is not None:
#                     dwell_ms = max(0, arr_ms - ld)
#                     out = {
#                         "metric": "dwell_time",
#                         "containerId": container,
#                         "dwell_seconds": int(dwell_ms / 1000),
#                         "arrival_ts": to_iso_from_epoch_ms(arr_ms)
#                     }
#                     # emit to main output as JSON string
#                     ctx.output(None, json.dumps(out))
#                     # clear states and delete timer
#                     t = self.timer_state.value()
#                     if t:
#                         ctx.timer_service().delete_event_time_timer(t)
#                     self.departure_state.clear()
#                     self.timer_state.clear()

#         def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
#             # Timer fired -> missing arrival
#             container_key = ctx.get_current_key()
#             alert = {
#                 "alert": "missing_arrival",
#                 "containerId": container_key,
#                 "details": f"No arrival seen within {MISSING_ARRIVAL_HOURS}h of departure",
#                 "timer_ts": to_iso_from_epoch_ms(timestamp)
#             }
#             ctx.output(alert_tag, json.dumps(alert))
#             # cleanup
#             self.departure_state.clear()
#             self.timer_state.clear()

#     dwell_stream = truck_parsed.key_by(lambda ev: ev.get("containerId") or "UNK") \
#         .process(TruckDwellProcess(), output_type=Types.STRING())

#     # ---------------- Analytics 3: Daily inspection failure rate ----------------
#     # Map to (date, total=1, flagged=1/0)
#     def insp_to_date_tuple(ev):
#         ts_ms = iso_to_epoch_ms(ev.get("timestamp"))
#         date = to_iso_from_epoch_ms(ts_ms)[:10]  # YYYY-MM-DD
#         result = ev.get("result")
#         flagged = 1 if result in ("FLAGGED", "HELD") else 0
#         return (date, 1, flagged, ts_ms)

#     insp_tuples = insp_parsed.map(insp_to_date_tuple, output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.LONG()]))

#     insp_by_date = insp_tuples.key_by(lambda t: t[0], key_type=Types.STRING())
#     daily_window = insp_by_date.window(TumblingEventTimeWindows.of(Time.days(1))) \
#         .reduce(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2], max(a[3], b[3])),
#                 output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.LONG()]))

#     def insp_agg_to_json(rec):
#         date, total, flagged, ts = rec
#         rate = (flagged / total) if total > 0 else 0.0
#         return json.dumps({"metric": "inspection_failure_rate", "date": date, "total": total, "flagged": flagged, "rate": rate})

#     insp_agg_json = daily_window.map(insp_agg_to_json, output_type=Types.STRING())

#     # Also create inspection alerts side-output for immediate FLAGGED/HELD
#     class InspectionAlertFn(ProcessFunction):
#         def process_element(self, ev, ctx: 'ProcessFunction.Context'):
#             result = ev.get("result")
#             if result in ("FLAGGED", "HELD"):
#                 alert = {
#                     "alert": "inspection_flag",
#                     "inspectionId": ev.get("inspectionId"),
#                     "containerId": ev.get("containerId"),
#                     "result": result,
#                     "notes": ev.get("notes"),
#                     "ts": ev.get("timestamp")
#                 }
#                 ctx.output(alert_tag, json.dumps(alert))
#             # also forward original event to main (unused here)
#             yield ev

#     # attach inspection alert generator (we only care about side outputs)
#     _ = insp_parsed.process(InspectionAlertFn(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

#     # Grab alerts side output from any stream we used InspectionAlertFn on or TruckDwellProcess
#     # Note: we used `alert_tag` in both TruckDwellProcess (ctx.output) and InspectionAlertFn
#     # To capture alerts we must get side output from a stream that has this tag. The easiest is:
#     # - create a merged "monitoring" stream that processes inspection events and truck events through those functions and captures side outputs
#     # Here we've already got inspection processing producing alerts. For truck dwell, alerts come from dwell_stream side output.
#     # To obtain the alerts side-output from dwell_stream we must have direct access:
#     dwell_alerts = dwell_stream.get_side_output(alert_tag)

#     # For inspection alerts we also created a side output on the processed inspection stream:
#     # But since we didn't keep a variable of the ProcessFunction output, let's re-run inspection through the function to get side output:
#     insp_alerts_holder = insp_parsed.process(InspectionAlertFn(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
#     insp_alerts = insp_alerts_holder.get_side_output(alert_tag)

#     # ---------------- Analytics 4: Staff shift durations ----------------
#     class StaffShiftProcess(KeyedProcessFunction):
#         """
#         key: staffId
#         Behavior:
#           - on START_SHIFT: store start_ms
#           - on END_SHIFT: compute duration, emit shift record
#           - on BREAK: ignore or handle as needed
#         """
#         def open(self, runtime_context: RuntimeContext):
#             self.start_state = runtime_context.get_state(ValueStateDescriptor("start", Types.LONG()))

#         def process_element(self, ev, ctx: 'KeyedProcessFunction.Context'):
#             etype = ev.get("eventType")
#             sid = ev.get("staffId")
#             ts_ms = iso_to_epoch_ms(ev.get("timestamp"))
#             if etype == "START_SHIFT":
#                 self.start_state.update(ts_ms)
#             elif etype == "END_SHIFT":
#                 start_ms = self.start_state.value()
#                 if start_ms is not None:
#                     duration_s = max(0, int((ts_ms - start_ms) / 1000))
#                     out = {
#                         "metric": "shift_duration",
#                         "staffId": sid,
#                         "duration_seconds": duration_s,
#                         "end_ts": to_iso_from_epoch_ms(ts_ms)
#                     }
#                     ctx.output(None, json.dumps(out))
#                     self.start_state.clear()
#             # BREAK events are ignored in this simple example
#     class EventTimestampAssigner(TimestampAssigner):
#         def extract_timestamp(self, value, record_timestamp):
#             if isinstance(value, dict):
#                 ts = value.get("timestamp") or value.get("departureTime") or value.get("arrivalTime")
#                 return iso_to_epoch_ms(ts)
#             return record_timestamp
        
#     staff_shift_stream = staff_parsed.key_by(lambda ev: ev.get("staffId") or "UNK") \
#         .process(StaffShiftProcess(), output_type=Types.STRING())

#     # ---------------- Union outputs and sinks ----------------
#     # We'll send KPI aggregates (prod_agg_json, dwell_stream main output, insp_agg_json, staff_shift_stream)
#     # Note: dwell_stream main output emits dwell JSON via ctx.output(None, json) so it's a main output
#     # Ensure types are all STRING
#     # dwell_stream is already STRING typed (process used output_type=Types.STRING())
#     results_union = prod_agg_json.union(dwell_stream, insp_agg_json, staff_shift_stream)

#     # Kafka sinks
#     agg_sink = create_kafka_sink(OUT_TOPIC_AGG, KAFKA_BOOTSTRAP)
#     alerts_sink = create_kafka_sink(OUT_TOPIC_ALERTS, KAFKA_BOOTSTRAP)

#     # Send aggregated KPI stream to analytics_agg
#     results_union.sink_to(agg_sink)

#     # Merge alerts from both sources (truck dwell alerts and inspection alerts)
#     all_alerts = dwell_alerts.union(insp_alerts)
#     all_alerts.sink_to(alerts_sink)

#     # Execute
#     env.execute("multi_topic_container_analytics_v1")


# if __name__ == "__main__":
#     main()

"""
multi_topic_container_analytics.py

PyFlink job that:
 - Consumes productEvent, truckEvent, staffEvent, inspectionEvent (each is its own Kafka topic)
 - Uses event-time with bounded out-of-orderness watermarks
 - Computes:
    * 5-minute throughput per warehouse (product events)
    * Dwell time per container (truck departure -> arrival) (stateful)
    * Daily inspection failure rate (inspection events)
    * Staff shift durations (stateful)
 - Emits alerts (side output) for flagged inspections and missing arrivals
 - Writes aggregates to analytics_agg and alerts to analytics_alerts Kafka topics
"""

import json
import logging
from datetime import datetime, timedelta
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common import Types, Time, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream import OutputTag

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("multi-topic-analytics")

# ---------------- CONFIG ----------------
KAFKA_BOOTSTRAP = "broker:29094"
GROUP_ID = "flink-multi-topic-analytics-v1"

TOPIC_PRODUCT = "productEvent"
TOPIC_TRUCK = "truckEvent"
TOPIC_STAFF = "staffEvent"
TOPIC_INSPECTION = "inspectionEvent"

OUT_TOPIC_AGG = "analytics_agg"
OUT_TOPIC_ALERTS = "analytics_alerts"

# how late events are tolerated (tweak to your data)
MAX_OUT_OF_ORDERNESS_SECONDS = 30

# If departure seen but no arrival within this many hours -> emit missing-arrival alert
MISSING_ARRIVAL_HOURS = 6

# ----------------- Helpers -----------------
def parse_json_safe(s: str):
    try:
        return json.loads(s)
    except Exception:
        logger.warning("Failed to parse JSON: %s", s)
        return None

def iso_to_epoch_ms(iso_ts: str):
    """
    Convert ISO 8601 string like "2025-11-04T12:34:56Z" to epoch ms.
    Falls back to current time if parse fails.
    """
    if not iso_ts:
        return int(datetime.utcnow().timestamp() * 1000)
    try:
        # Accepts "YYYY-MM-DDTHH:MM:SSZ"
        return int(datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%SZ").timestamp() * 1000)
    except Exception:
        # Try a few flexible parses if necessary
        try:
            return int(datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            logger.warning("timestamp parse failed for %s", iso_ts)
            return int(datetime.utcnow().timestamp() * 1000)

def to_iso_from_epoch_ms(ms: int):
    return datetime.utcfromtimestamp(ms/1000).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------- Timestamp Assigner -----------------
class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        # value is expected to be a dict (parsed JSON); fallback to record_timestamp
        if isinstance(value, dict):
            ts = value.get("timestamp") or value.get("departureTime") or value.get("arrivalTime")
            return iso_to_epoch_ms(ts)
        return record_timestamp

# ---------------- Execution ----------------
def create_kafka_source(topic: str, bootstrap: str, group_id: str):
    return KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap) \
        .set_group_id(group_id) \
        .set_topics(topic) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

def create_kafka_sink(topic: str, bootstrap: str):
    return KafkaSink.builder() \
        .set_bootstrap_servers(bootstrap) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    # optional: env.set_stream_time_characteristic(TimeCharacteristic.EventTime)  # older versions
    env.get_config().set_auto_watermark_interval(1000)

    # ---------------- Watermark Strategy (common) ----------------
    wm_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(MAX_OUT_OF_ORDERNESS_SECONDS)) \
        .with_timestamp_assigner(EventTimestampAssigner())

    # ---------------- Sources ----------------
    prod_src = create_kafka_source(TOPIC_PRODUCT, KAFKA_BOOTSTRAP, GROUP_ID)
    truck_src = create_kafka_source(TOPIC_TRUCK, KAFKA_BOOTSTRAP, GROUP_ID)
    staff_src = create_kafka_source(TOPIC_STAFF, KAFKA_BOOTSTRAP, GROUP_ID)
    insp_src = create_kafka_source(TOPIC_INSPECTION, KAFKA_BOOTSTRAP, GROUP_ID)

    raw_prod = env.from_source(prod_src, wm_strategy, "product-source", type_info=Types.STRING())
    raw_truck = env.from_source(truck_src, wm_strategy, "truck-source", type_info=Types.STRING())
    raw_staff = env.from_source(staff_src, wm_strategy, "staff-source", type_info=Types.STRING())
    raw_insp = env.from_source(insp_src, wm_strategy, "inspection-source", type_info=Types.STRING())

    # ---------------- Parse JSON ----------------
    prod_parsed = raw_prod.map(lambda s: parse_json_safe(s), output_type=Types.MAP(Types.STRING(), Types.STRING())).filter(lambda x: x is not None)
    truck_parsed = raw_truck.map(lambda s: parse_json_safe(s), output_type=Types.MAP(Types.STRING(), Types.STRING())).filter(lambda x: x is not None)
    staff_parsed = raw_staff.map(lambda s: parse_json_safe(s), output_type=Types.MAP(Types.STRING(), Types.STRING())).filter(lambda x: x is not None)
    insp_parsed = raw_insp.map(lambda s: parse_json_safe(s), output_type=Types.MAP(Types.STRING(), Types.STRING())).filter(lambda x: x is not None)

    # ---------------- Analytics 1: 5-min throughput per warehouse (product events) ----------------
    # map -> (destination, 1, ts)
    prod_kv = prod_parsed.map(lambda ev: (ev.get("destination") or "UNK", 1, iso_to_epoch_ms(ev.get("timestamp"))),
                               output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()]))

    from pyflink.datastream.window import TumblingEventTimeWindows
    prod_by_wh = prod_kv.key_by(lambda t: t[0], key_type=Types.STRING())
    prod_windowed = prod_by_wh.window(TumblingEventTimeWindows.of(Time.minutes(5))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1], max(a[2], b[2])),
                output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()]))

    def prod_agg_to_json(rec):
        warehouse, cnt, ts = rec
        return json.dumps({
            "metric": "throughput",
            "warehouse": warehouse,
            "count": cnt,
            "window_end_ts": to_iso_from_epoch_ms(ts)
        })

    prod_agg_json = prod_windowed.map(prod_agg_to_json, output_type=Types.STRING())

    # ---------------- Analytics 2: Dwell time per container (truck events: departure -> arrival) ----------------
    alert_tag = OutputTag("alerts", Types.STRING())

    class TruckDwellProcess(KeyedProcessFunction):
        """
        key: containerId
        Behavior:
          - on departureTime: store departure_ms in state, register timer at departure_ms + MISSING_ARRIVAL_HOURS
          - on arrivalTime: if departure exists -> compute dwell and emit; clear state and delete timer
          - timer callback: if fired and arrival not seen -> emit missing-arrival alert and clear state
        """
        def open(self, runtime_context: RuntimeContext):
            self.departure_state = runtime_context.get_state(ValueStateDescriptor("departure", Types.LONG()))
            self.timer_state = runtime_context.get_state(ValueStateDescriptor("timer_ts", Types.LONG()))

        def process_element(self, ev, ctx: 'KeyedProcessFunction.Context'):
            # ev is a map
            dep = ev.get("departureTime")
            arr = ev.get("arrivalTime")
            container = ev.get("containerId") or "UNK"

            if dep:
                dep_ms = iso_to_epoch_ms(dep)
                self.departure_state.update(dep_ms)
                # register event-time timer for missing arrival
                timer_ms = dep_ms + int(MISSING_ARRIVAL_HOURS * 3600 * 1000)
                ctx.timer_service().register_event_time_timer(timer_ms)
                self.timer_state.update(timer_ms)
            if arr:
                arr_ms = iso_to_epoch_ms(arr)
                ld = self.departure_state.value()
                if ld is not None:
                    dwell_ms = max(0, arr_ms - ld)
                    out = {
                        "metric": "dwell_time",
                        "containerId": container,
                        "dwell_seconds": int(dwell_ms / 1000),
                        "arrival_ts": to_iso_from_epoch_ms(arr_ms)
                    }
                    # emit to main output as JSON string
                    # for KeyedProcessFunction, collect via ctx.output(None, ...) is used for side outputs;
                    # to emit to main output, the typical approach depends on API; many existing examples use ctx.output(None,...)
                    # Keep using ctx.output(None, ...) as in original code to preserve behavior and types.
                    ctx.output(None, json.dumps(out))
                    # clear states and delete timer
                    t = self.timer_state.value()
                    if t:
                        ctx.timer_service().delete_event_time_timer(t)
                    self.departure_state.clear()
                    self.timer_state.clear()

        def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
            # Timer fired -> missing arrival
            container_key = ctx.get_current_key()
            alert = {
                "alert": "missing_arrival",
                "containerId": container_key,
                "details": f"No arrival seen within {MISSING_ARRIVAL_HOURS}h of departure",
                "timer_ts": to_iso_from_epoch_ms(timestamp)
            }
            ctx.output(alert_tag, json.dumps(alert))
            # cleanup
            self.departure_state.clear()
            self.timer_state.clear()

    dwell_stream = truck_parsed.key_by(lambda ev: ev.get("containerId") or "UNK") \
        .process(TruckDwellProcess(), output_type=Types.STRING())

    # ---------------- Analytics 3: Daily inspection failure rate ----------------
    # Map to (date, total=1, flagged=1/0)
    def insp_to_date_tuple(ev):
        ts_ms = iso_to_epoch_ms(ev.get("timestamp"))
        date = to_iso_from_epoch_ms(ts_ms)[:10]  # YYYY-MM-DD
        result = ev.get("result")
        flagged = 1 if result in ("FLAGGED", "HELD") else 0
        return (date, 1, flagged, ts_ms)

    insp_tuples = insp_parsed.map(insp_to_date_tuple, output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.LONG()]))

    insp_by_date = insp_tuples.key_by(lambda t: t[0], key_type=Types.STRING())
    daily_window = insp_by_date.window(TumblingEventTimeWindows.of(Time.days(1))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2], max(a[3], b[3])),
                output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.LONG()]))

    def insp_agg_to_json(rec):
        date, total, flagged, ts = rec
        rate = (flagged / total) if total > 0 else 0.0
        return json.dumps({"metric": "inspection_failure_rate", "date": date, "total": total, "flagged": flagged, "rate": rate})

    insp_agg_json = daily_window.map(insp_agg_to_json, output_type=Types.STRING())

    # Also create inspection alerts side-output for immediate FLAGGED/HELD
    class InspectionAlertFn(ProcessFunction):
        def process_element(self, ev, ctx: 'ProcessFunction.Context'):
            result = ev.get("result")
            if result in ("FLAGGED", "HELD"):
                alert = {
                    "alert": "inspection_flag",
                    "inspectionId": ev.get("inspectionId"),
                    "containerId": ev.get("containerId"),
                    "result": result,
                    "notes": ev.get("notes"),
                    "ts": ev.get("timestamp")
                }
                ctx.output(alert_tag, json.dumps(alert))
            # also forward original event to main (unused here)
            yield ev

    # attach inspection alert generator (we only care about side outputs)
    inspection_processed = insp_parsed.process(InspectionAlertFn(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # Grab alerts side outputs
    dwell_alerts = dwell_stream.get_side_output(alert_tag)
    insp_alerts = inspection_processed.get_side_output(alert_tag)

    # ---------------- Analytics 4: Staff shift durations ----------------
    class StaffShiftProcess(KeyedProcessFunction):
        """
        key: staffId
        Behavior:
          - on START_SHIFT: store start_ms
          - on END_SHIFT: compute duration, emit shift record
          - on BREAK: ignore or handle as needed
        """
        def open(self, runtime_context: RuntimeContext):
            self.start_state = runtime_context.get_state(ValueStateDescriptor("start", Types.LONG()))

        def process_element(self, ev, ctx: 'KeyedProcessFunction.Context'):
            etype = ev.get("eventType")
            sid = ev.get("staffId")
            ts_ms = iso_to_epoch_ms(ev.get("timestamp"))
            if etype == "START_SHIFT":
                self.start_state.update(ts_ms)
            elif etype == "END_SHIFT":
                start_ms = self.start_state.value()
                if start_ms is not None:
                    duration_s = max(0, int((ts_ms - start_ms) / 1000))
                    out = {
                        "metric": "shift_duration",
                        "staffId": sid,
                        "duration_seconds": duration_s,
                        "end_ts": to_iso_from_epoch_ms(ts_ms)
                    }
                    ctx.output(None, json.dumps(out))
                    self.start_state.clear()
            # BREAK events are ignored in this simple example

    staff_shift_stream = staff_parsed.key_by(lambda ev: ev.get("staffId") or "UNK") \
        .process(StaffShiftProcess(), output_type=Types.STRING())

    # ---------------- Union outputs and sinks ----------------
    # We'll send KPI aggregates (prod_agg_json, dwell_stream main output, insp_agg_json, staff_shift_stream)
    results_union = prod_agg_json.union(dwell_stream, insp_agg_json, staff_shift_stream)

    # Kafka sinks
    agg_sink = create_kafka_sink(OUT_TOPIC_AGG, KAFKA_BOOTSTRAP)
    alerts_sink = create_kafka_sink(OUT_TOPIC_ALERTS, KAFKA_BOOTSTRAP)

    # Send aggregated KPI stream to analytics_agg
    results_union.sink_to(agg_sink)

    # Merge alerts from both sources (truck dwell alerts and inspection alerts)
    all_alerts = dwell_alerts.union(insp_alerts)
    all_alerts.sink_to(alerts_sink)

    # Execute
    env.execute("multi_topic_container_analytics_v1")


if __name__ == "__main__":
    main()
