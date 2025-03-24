Flink_UI: http://localhost:8081
Airflow_UI: http://localhost:8080  Username: admin   Password: admin
Cassandra :docker exec -it fraud-cassandra-1 cqlsh
Start_Zookeeper: ~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties &
Start_Kafka_Broker: ~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties &
list_all_topics : ~/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
downloawd_jar: wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar -P /tmp/
run : JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 /home/ali/Desktop/fraud/venv/bin/python /home/ali/Desktop/fraud/src/load.py
check: ls /tmp/fraud_output/