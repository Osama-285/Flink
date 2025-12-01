from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common import Types
from datetime import datetime
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.state import MapStateDescriptor


MAX_AMOUNT = 1000
RAPID_SECONDS = 10
RAPID_COUNT = 3
SUSPICIOUS_LOCATIONS = {'TX', 'FL'}

class FraudDetector(KeyedProcessFunction):

    def open(self, ctx):
        descriptor = MapStateDescriptor(
            "transaction_history",
            Types.LONG(),
            Types.TUPLE([Types.FLOAT(), Types.STRING()])
        )
        self.tx_state = ctx.get_map_state(descriptor)


    def process_element(self, event, ctx):
        customer = event.customer_id
        amount = float(event.amount)
        location = event.location
        timestamp = int(datetime.fromisoformat(event.timestamp.replace("Z", "")).timestamp())

        alerts = []

        # store event in state
        self.tx_state.put(timestamp, (amount, location))

        # remove old events beyond window
        to_delete = []
        for ts in self.tx_state.keys():
            if timestamp - ts > RAPID_SECONDS:
                to_delete.append(ts)
        for ts in to_delete:
            self.tx_state.remove(ts)

        if amount > MAX_AMOUNT:
            alerts.append(f"⚠️ High amount: ${amount} for {customer}")

        recent_tx_count = sum(
            1 for ts in self.tx_state.keys()
            if timestamp - ts <= RAPID_SECONDS
        )
        if recent_tx_count >= RAPID_COUNT:
            alerts.append(
                f"🚨 Rapid usage: {recent_tx_count} tx in {RAPID_SECONDS} sec for {customer}"
            )

        if location in SUSPICIOUS_LOCATIONS:
            alerts.append(f"🚩 Suspicious location: {location} for {customer}")

        for alert in alerts:
            print(alert)

        return event


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker:29092")
        .set_topics("transactions")
        .set_group_id("fraud-detector")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder().type_info(
                Types.ROW_NAMED(
                    ["customer_id", "transaction_id", "amount", "location", "timestamp"],
                    [Types.STRING(), Types.INT(), Types.FLOAT(), Types.STRING(), Types.STRING()]
                )
            ).build()
        )
        .build()
    )

    stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),       # no watermarks for now
        source_name="KafkaSource"
    )

    stream \
        .key_by(lambda row: row.customer_id) \
        .process(FraudDetector()) \
        .print()

    env.execute("fraud-detection-system")


if __name__ == "__main__":
    main()
