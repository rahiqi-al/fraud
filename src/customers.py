import logging
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, TableDescriptor, Schema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.types import DataTypes

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename="logs/app.log")
logger = logging.getLogger(__name__)

OUTPUT_DIR = '/tmp/fraud_output'

def customer():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    stream_env.set_parallelism(1)
    stream_env.enable_checkpointing(5000)
    stream_env.add_jars("file:///home/ali/Desktop/fraud/venv/lib/python3.11/site-packages/pyflink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    env = StreamTableEnvironment.create(stream_env, environment_settings=env_settings)

    env.create_temporary_table('customers_source', TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('customer_id', DataTypes.STRING())
                .column('account_history', DataTypes.STRING())
                .column('demographics_age', DataTypes.INT())
                .column('demographics_location', DataTypes.STRING())
                .column('behavioral_patterns_avg_transaction_value', DataTypes.DOUBLE())
                .build())
        .option('properties.bootstrap.servers', 'localhost:9092')
        .option('properties.group.id', 'flink-consumer')
        .option('topic', 'customers')
        .option('scan.startup.mode', 'earliest-offset')
        .format('json')
        .option('json.fail-on-missing-field', 'false')
        .build())

    env.create_temporary_table('customers_sink', TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('customer_id', DataTypes.STRING())
                .column('account_history', DataTypes.STRING())
                .column('demographics_age', DataTypes.INT())
                .column('demographics_location', DataTypes.STRING())
                .column('behavioral_patterns_avg_transaction_value', DataTypes.DOUBLE())
                .build())
        .option('path', f'{OUTPUT_DIR}/customers.csv')
        .format('csv')
        .build())

    env.sql_query("SELECT * FROM customers_source").execute_insert('customers_sink').wait()
    logger.info("Customers sink completed")

    stream_env.execute("Kafka to Customers CSV")

customer()