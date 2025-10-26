import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

def parse_json_or_print(value):
    try:
        data = json.loads(value)
        # you can process valid data here if needed
        return f"VALID: {data}"
    except json.JSONDecodeError as e:
        # print invalid JSON for debugging
        print(f"❌ Invalid JSON: {value}")
        print(f"   Error: {str(e)}")
        return f"INVALID: {value}"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(5)

    # Kafka Source - reading raw string data
    consumer = FlinkKafkaConsumer(
        topics='shipEvent',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'broker:29094',
            'group.id': 'flink_json_debug'
        }
    )

    stream = env.add_source(consumer)

    # Try to parse each record as JSON, print invalids
    stream.map(parse_json_or_print).print()

    env.execute("Print Invalid JSON Records")

if __name__ == "__main__":
    main()
