import json
import logging
from aiokafka import AIOKafkaProducer
from core.config import settings

logger = logging.getLogger(__name__)

_producer: AIOKafkaProducer | None = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            enable_idempotence=True,
        )
        await _producer.start()
        logger.info("Kafka producer started")
    return _producer


async def stop_producer():
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None
        logger.info("Kafka producer stopped")


async def publish_event(topic: str, key: str, payload: dict):
    producer = await get_producer()
    await producer.send_and_wait(topic, key=key, value=payload)
    logger.info(f"Published to [{topic}] key={key} event={payload.get('event_type')}")