from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from datetime import datetime, timedelta
from pyflink.common import WatermarkStrategy



env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)

source = KafkaSource.builder() \
    .set_bootstrap_servers("broker:29094") \
    .set_topics("containerEvent") \
    .set_group_id("in_port_tracker") \
    .set_value_only_deserializer(
        JsonRowDeserializationSchema.builder()
        .type_info(Types.ROW_NAMED(
            ["containerId", "status", "timestamp"],
            [Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP()]
        )).build()
    ).build()

ds = env.from_source(source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="container_events")

def in_port_tracker():
    state = {}

    def process(value):
        container_id, status, ts = value
        # Update state
        state[container_id] = (status, ts)

        # Remove containers not in port
        state_filtered = {k: v for k, v in state.items() if v[0] == "IN_PORT"}
        state.clear()
        state.update(state_filtered)

        # Alert for containers IN_PORT > 24h
        now = datetime.utcnow()
        for cid, (s, t) in state.items():
            if (now - t) > timedelta(hours=24):
                print(f"ALERT: {cid} has been IN_PORT > 24h")

        print(f"Current IN_PORT containers: {len(state)}")
    return process

ds.map(in_port_tracker())

# ---------------------- Execute ----------------------
env.execute("AdvancedContainerAnalyticsPipeline")