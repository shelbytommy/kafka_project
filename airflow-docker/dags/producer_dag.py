import os
from datetime import datetime
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import pandas as pd

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer


def test_version():
    import confluent_kafka

    print(f"version of confluent_kafka: {confluent_kafka.__version__}")


def emit_data():
    # data_dir = /opt/airflow/dags/data
    df = pd.read_csv(
        os.path.join(
            Variable.get("data_dir"),
            'Waterbase_v2018_1_T_WISE4_BiologyEQRData.csv',
            # 'Waterbase_v2018_1_T_WISE4_AggregatedData.csv',
        )
    )
    kafka_config = {
        'bootstrap.servers': 'MBP-tommy:9092',
        'client.id': 'data_quality_producer',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': StringSerializer('utf_8'),
        'batch.num.messages': 100,
    }

    topic = 'water_quality_data'
    producer = SerializingProducer(kafka_config)

    for _, row in df.iterrows():
        value = json.dumps(row.to_dict())

        producer.produce(topic, key=str(row["UID"]), value=value)

    producer.flush()

    print(f"Data published to Kafka topic: {topic}")


with DAG(
    "producer_dag",
    start_date=datetime(2023, 10, 20),
    schedule_interval=None,
    catchup=False,
) as dag:
    test_version_task = PythonOperator(
        task_id="test_version", python_callable=test_version
    )
    emit_data_task = PythonOperator(
        task_id="emit_water_quality_data", python_callable=emit_data
    )

    test_version_task >> emit_data_task
