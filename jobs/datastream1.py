from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types, Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import ReduceFunction
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # JSON deserializer with schema
    deserializer = JsonRowDeserializationSchema.builder() \
        .type_info(
            Types.ROW_NAMED(
                ["account_id", "amount", "timestamp"],
                [Types.STRING(), Types.FLOAT(), Types.SQL_TIMESTAMP()]
            )
        ).build()

    # Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29092").set_topics("bank.transactions") \
        .set_group_id("flink-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(deserializer) \
        .build()

    # Source with watermark strategy (event time enabled automatically)
    ds = env.from_source(
        source=kafka_source,
        watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)),
        source_name="Kafka Source"
    )

    # Classify transactions as FRAUD/GENUINE
    classified = ds.map(
        lambda row: ("FRAUD" if row[1] > 1000 else "GENUINE", 1),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    )

    # Key by classification
    keyed = classified.key_by(lambda row: row[0], key_type=Types.STRING())

    # Apply 30s tumbling window
    windowed = keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))

    class CountReduce(ReduceFunction):
        def reduce(self, v1, v2):
            return (v1[0], v1[1] + v2[1])

    # Aggregate counts
    aggregated = windowed.reduce(
        CountReduce(),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    )

    aggregated.print()

    env.execute("Fraud Detection with Kafka - Flink 2.0")


if __name__ == "__main__":
    main()
