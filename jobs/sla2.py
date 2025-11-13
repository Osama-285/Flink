# sla_violation_monitor.py

import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource,KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy


def parse_json(value):
    try:
        return json.loads(value)
    except:
        return None


def is_sla_violation(event):
    if event is None:
        return False
    return event.get("processingMinutes", 0) > 30


def format_alert(event):
    return (
        f"⚠️ SLA VIOLATION: container={event['containerId']} "
        f"took {event['processingMinutes']} minutes (threshold=30)"
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker:29094")
        .set_topics("tinspectionEvent")
        .set_group_id("sla-monitor-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "inspection-source")
        .map(parse_json)              
        .filter(is_sla_violation)
        .map(format_alert)            
        .print()                     
    )

    env.execute("SLA Violation Monitor")


if __name__ == "__main__":
    main()
