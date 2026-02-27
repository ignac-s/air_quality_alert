from kafka import KafkaProducer
import json
from loguru import logger

TOPIC_NAME = "air_quality_raw"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_kafka(data: list[dict]) -> None:
    for record in data:
        try:
            producer.send(TOPIC_NAME, record)
            logger.info(f"Sent to Kafka: {record}")
        except Exception as e:
            logger.error(f"Failed to send record: {e}")
