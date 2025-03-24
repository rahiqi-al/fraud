import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename="logs/app.log")
logger = logging.getLogger(__name__)

def on_failure_callback(context):
    task_instance = context['task_instance']
    logger.error(f"Task {task_instance.task_id} failed. DAG: {task_instance.dag_id} at {datetime.now()}")

default_args = {
    'owner': 'ali rahiqi',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

java_home = "JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"
python_path = "/home/ali/Desktop/fraud/venv/bin/python"
script_dir = "/home/ali/Desktop/fraud/src"
with DAG(
    dag_id='fraud',
    default_args=default_args,
    start_date=datetime(2025, 3, 22),
    schedule_interval='0 2 * * *',
    catchup=False,
    on_failure_callback=on_failure_callback
) as dag:

    transaction_task = BashOperator(
        task_id='transaction',
        bash_command=f"{java_home} {python_path} {script_dir}/load.py",
    )

    customer_task = BashOperator(
        task_id='customer',
        bash_command=f"{java_home} {python_path} {script_dir}/customers.py",
    )

    external_data_task = BashOperator(
        task_id='external_data',
        bash_command=f"{java_home} {python_path} {script_dir}/external_data.py",
    )

    fraud_task = BashOperator(
        task_id='fraud',
        bash_command=f"{java_home} {python_path} {script_dir}/fraud.py",
    )

    [transaction_task, customer_task, external_data_task, fraud_task]