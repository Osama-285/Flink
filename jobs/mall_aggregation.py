from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    # -------------------------------------------------------------------
    # Flink Environment Setup
    # -------------------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # -------------------------------------------------------------------
    # Kafka Source Table
    # -------------------------------------------------------------------
    topics = ['mallFloor1','mallFloor2','mallFloor3','mallFloor4','mallFloor5']
    # topics = ['mallFloor1', 'mallFloor2', 'mallFloor3', 'mallFloor4', 'mallFloor5']
# Convert the list of topics to a semicolon-separated string
    kafka_topics = ';'.join(topics)
    t_env.execute_sql(f"""
        CREATE TABLE mall_transactions (
            floor_number INT,
            shop_number INT,
            transaction_id STRING,
            transaction_amount DOUBLE,
            transaction_date TIMESTAMP(3),
            shop_type STRING,
            WATERMARK FOR transaction_date AS transaction_date - INTERVAL '5' SECOND
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{kafka_topics}',
          'properties.bootstrap.servers' = 'broker:29094',
          'properties.group.id' = 'flink-mall-data-consumer',
          'scan.startup.mode' = 'earliest-offset',
          'format' = 'json',
          'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)
    # t_env.execute_sql("""
    #     CREATE TABLE print_sink (
    #         floor_number INT,
    #         shop_number INT,
    #         transaction_id STRING,
    #         transaction_amount DOUBLE,
    #         transaction_date TIMESTAMP(3),
    #         shop_type STRING
    #     ) WITH (
    #       'connector' = 'print'
    #     )
    # """)

    # # -------------------------------------------------------------------
    # # Pipe data from Kafka → Print
    # # -------------------------------------------------------------------
    # t_env.execute_sql("""
    #     INSERT INTO print_sink
    #     SELECT * FROM mall_transactions
    # """)

    # -------------------------------------------------------------------
    # PostgreSQL Sink Table 1: Per Shop
    # -------------------------------------------------------------------
    t_env.execute_sql("""
        CREATE TABLE shop_aggregates (
            txn_day DATE,
            shop_number INT,
            total_transactions BIGINT,
            total_amount DOUBLE,
            PRIMARY KEY (txn_day, shop_number) NOT ENFORCED
        ) WITH (
          'connector' = 'jdbc',
          'url' = 'jdbc:postgresql://host.docker.internal:5000/dbt_db',
          'table-name' = 'public.shop_aggregates',
          'username' = 'dbtuser',
          'password' = 'test123',
          'driver' = 'org.postgresql.Driver'
        )
    """)

    # -------------------------------------------------------------------
    # PostgreSQL Sink Table 2: Per Floor
    # -------------------------------------------------------------------
    t_env.execute_sql("""
        CREATE TABLE floor_aggregates (
            txn_day DATE,
            floor_number INT,
            total_transactions BIGINT,
            total_amount DOUBLE,
            PRIMARY KEY (txn_day, floor_number) NOT ENFORCED
        ) WITH (
          'connector' = 'jdbc',
          'url' = 'jdbc:postgresql://host.docker.internal:5000/dbt_db',
          'table-name' = 'public.floor_aggregates',
          'username' = 'dbtuser',
          'password' = 'test123',
          'driver' = 'org.postgresql.Driver'
        )
    """)

    # -------------------------------------------------------------------
    # Query 1: Per Shop Aggregates
    # -------------------------------------------------------------------
    t_env.execute_sql("""
        INSERT INTO shop_aggregates
        SELECT
            CAST(transaction_date AS DATE) AS txn_day,
            shop_number,
            COUNT(*) AS total_transactions,
            SUM(transaction_amount) AS total_amount
        FROM mall_transactions
        GROUP BY CAST(transaction_date AS DATE), shop_number
    """)

    # -------------------------------------------------------------------
    # Query 2: Per Floor Aggregates
    # -------------------------------------------------------------------
    t_env.execute_sql("""
        INSERT INTO floor_aggregates
        SELECT
            CAST(transaction_date AS DATE) AS txn_day,
            floor_number,
            COUNT(*) AS total_transactions,
            SUM(transaction_amount) AS total_amount
        FROM mall_transactions
        GROUP BY CAST(transaction_date AS DATE), floor_number
    """)

if __name__ == "__main__":
    main()
