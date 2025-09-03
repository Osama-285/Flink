from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy, Time
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from datetime import datetime, timedelta
from pyflink.common import Time, Duration


class FraudGenuineWindow(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        fraud_count = 0
        genuine_count = 0
        for label, count in elements:
            if label == "FRAUD":
                fraud_count += count
            else:
                genuine_count += count
        start = datetime.fromtimestamp(context.window().start / 1000.0)
        end = datetime.fromtimestamp(context.window().end / 1000.0)
        out.collect(f"| FRAUD: {fraud_count} | GENUINE: {genuine_count} | "
                    f"START: {start} | END: {end} |")

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    # JSON deserializer for Kafka
    deserializer = JsonRowDeserializationSchema.builder() \
        .type_info(
            Types.ROW_NAMED(
                ["account_id", "amount", "timestamp"],
                [Types.STRING(), Types.FLOAT(), Types.SQL_TIMESTAMP()]
            )
        ).build()

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_topics("bank.transactions") \
        .set_group_id("flink-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(deserializer) \
        .build()

    ds = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ),
        source_name="Kafka Source"
    )

    # classify
    classified = ds.map(
        lambda row: ("FRAUD" if row[1] > 1000 else "GENUINE", 1),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    )

    # key by dummy key (so all results go to one window function)
    keyed = classified.key_by(lambda row: "ALL", key_type=Types.STRING())

    windowed = keyed.window(TumblingEventTimeWindows.of(Time.seconds(30)))

    aggregated = windowed.process(
        FraudGenuineWindow(),
        output_type=Types.STRING()
    )

    aggregated.print()

    env.execute("Fraud vs Genuine Windowed Counts")


if __name__ == "__main__":
    main()
