import logging
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, TableDescriptor, Schema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.types import DataTypes

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename="logs/app.log")
logger = logging.getLogger(__name__)

OUTPUT_DIR = '/tmp/fraud_output'

def fraud():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    stream_env.set_parallelism(1)
    stream_env.enable_checkpointing(5000)
    stream_env.add_jars("file:///home/ali/Desktop/fraud/venv/lib/python3.11/site-packages/pyflink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    env = StreamTableEnvironment.create(stream_env, environment_settings=env_settings)

    env.create_temporary_table('transactions_source', TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('transaction_id', DataTypes.STRING())
                .column('date_time', DataTypes.STRING())
                .column('amount', DataTypes.DOUBLE())
                .column('currency', DataTypes.STRING())
                .column('merchant_details', DataTypes.STRING())
                .column('customer_id', DataTypes.STRING())
                .column('transaction_type', DataTypes.STRING())
                .column('location', DataTypes.STRING())
                .build())
        .option('properties.bootstrap.servers', 'localhost:9092')
        .option('properties.group.id', 'flink-consumer')
        .option('topic', 'transactions')
        .option('scan.startup.mode', 'earliest-offset')
        .format('json')
        .option('json.fail-on-missing-field', 'false')
        .build())

    env.create_temporary_table('fraud_sink', TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('transaction_id', DataTypes.STRING())
                .column('date_time', DataTypes.STRING())
                .column('amount', DataTypes.DOUBLE())
                .column('currency', DataTypes.STRING())
                .column('merchant_details', DataTypes.STRING())
                .column('customer_id', DataTypes.STRING())
                .column('transaction_type', DataTypes.STRING())
                .column('location', DataTypes.STRING())
                .build())
        .option('path', f'{OUTPUT_DIR}/fraud.csv')
        .format('csv')
        .build())

    fraud_query = """
    SELECT transaction_id, date_time, amount, currency, merchant_details, customer_id, transaction_type, location
    FROM transactions_source
    WHERE amount > 5000
    """
    env.sql_query(fraud_query).execute_insert('fraud_sink').wait()
    logger.info("Fraud sink completed")

    stream_env.execute("Kafka to Fraud CSV")

fraud()