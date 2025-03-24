import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.config import config
import logging
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient , NewTopic
import json
import time
import random
from datetime import datetime, timedelta
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename="logs/app.log")
logger = logging.getLogger(__name__)


def report(err, msg):
    if err is not None:
        logger.error(f'message failed from the producer: {err}')
    else:
        logger.info(f'message delivered to: topic = {msg.topic()} & partition = {msg.partition()}')

def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def generate_data():
    customers = []
    for i in range(100):
        customer_id = f"C{i:03}"
        customers.append({
            "customer_id": customer_id,
            "account_history": [],
            "demographics": {"age": random.randint(18, 70), "location": f"City{random.randint(1, 10)}"},
            "behavioral_patterns": {"avg_transaction_value": random.uniform(50, 500)}
        })

    transactions = []
    for i in range(1000):
        customer_id = f"C{random.randint(0, 99):03}"
        transaction = {
            "transaction_id": f"T{i:05}",
            "date_time": random_date(datetime(2020, 1, 1), datetime(2023, 1, 1)).isoformat(),
            "amount": random.uniform(10, 1000) * (10 if random.random() < 0.4 else 1),
            "currency": random.choice(["USD", "EUR", "GBP"]),
            "merchant_details": f"Merchant{random.randint(1, 20)}",
            "customer_id": customer_id,
            "transaction_type": random.choice(["purchase", "withdrawal"]),
            "location": f"City{random.randint(1, 10)}"
        }
        transactions.append(transaction)
        for c in customers:
            if c["customer_id"] == customer_id:
                c["account_history"].append(transaction["transaction_id"])
                break

    external_data = {
        "blacklist_info": [f"Merchant{random.randint(21, 30)}" for _ in range(10)],
        "credit_scores": {c["customer_id"]: random.randint(300, 850) for c in customers},
        "fraud_reports": {c["customer_id"]: random.randint(0, 5) for c in customers}
    }

    logger.info('data generated successfully')
    return transactions, customers, external_data

def producer():
    try:
        #create topics if they dont exists
        admin = AdminClient(config.kafka_config)
        existing_topics = admin.list_topics().topics
        for topic_name in config.topics.values():
            if topic_name not in existing_topics:
                topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
                admin.create_topics([topic])
                logger.info(f'topic got created under the name: {topic_name}')
            else:
                logger.info(f'topic already exists: {topic_name}')

        producer = Producer(config.kafka_config)

        while True:
            transactions, customers, external_data = generate_data()
            
            #send transactions
            for t in transactions:
                producer.produce(config.topics["transactions"], value=json.dumps(t).encode('utf-8'), callback=report)
            #send customers
            for c in customers:
                producer.produce(config.topics["customers"], value=json.dumps(c).encode('utf-8'), callback=report)
            #send  external data
            producer.produce(config.topics["external_data"], value=json.dumps(external_data).encode('utf-8'), callback=report)
            
            producer.flush()
            logger.info('data sent to Kafka')
            time.sleep(10)  

    except Exception as e:
        logger.exception('producer error')



producer()