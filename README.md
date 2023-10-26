# Water data quality project - kafka streaming

### List of dockers

**Zookeeper**
```bash
docker run --name zookeeper -p 2181:2181 zookeeper
```

**Kafka**
```bash
docker run --name kafka -p 9092:9092
-e KAFKA_ZOOKEEPER_CONNECT={localhost}:2181
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{localhost}:9092
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
confluentinc/cp-kafka
```

**Jupyter notebook**
```bash
docker run --name jupyter -p 8888:8888 jupyter/datascience-notebook:latest
```

**Airflow**
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.2/docker-compose.yaml'

docker-compose up
```

**When you need to add some packages you might find useful**
```bash
docker build . --tag extending_airflow:latest

docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
```

