import logging
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, TableDescriptor, Schema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.types import DataTypes

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename="logs/app.log")
logger = logging.getLogger(__name__)

OUTPUT_DIR = '/tmp/fraud_output'

def external_data():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    stream_env.set_parallelism(1)
    stream_env.enable_checkpointing(5000)
    stream_env.add_jars("file:///home/ali/Desktop/fraud/venv/lib/python3.11/site-packages/pyflink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    env = StreamTableEnvironment.create(stream_env, environment_settings=env_settings)

    env.create_temporary_table('external_data_source', TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('blacklist_info', DataTypes.STRING())
                .column('credit_scores', DataTypes.STRING())
                .column('fraud_reports', DataTypes.STRING())
                .build())
        .option('properties.bootstrap.servers', 'localhost:9092')
        .option('properties.group.id', 'flink-consumer')
        .option('topic', 'external_data')
        .option('scan.startup.mode', 'earliest-offset')
        .format('json')
        .option('json.fail-on-missing-field', 'false')
        .build())

    env.create_temporary_table('external_data_sink', TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('blacklist_info', DataTypes.STRING())
                .column('credit_scores', DataTypes.STRING())
                .column('fraud_reports', DataTypes.STRING())
                .build())
        .option('path', f'{OUTPUT_DIR}/external_data.csv')
        .format('csv')
        .build())

    env.sql_query("SELECT * FROM external_data_source").execute_insert('external_data_sink').wait()
    logger.info("External data sink completed")

    stream_env.execute("Kafka to External Data CSV")

external_data()