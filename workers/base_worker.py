import json
import logging
import asyncio
from aiokafka import AIOKafkaConsumer
from core.config import settings

logger = logging.getLogger(__name__)


class BaseWorker:
    """
    Base class for all Kafka consumer workers.
    Each subclass defines which topic(s) to consume and implements `handle_message`.
    """

    consumer_group: str = "base-group"
    topics: list[str] = []

    def __init__(self):
        self.consumer: AIOKafkaConsumer | None = None
        self.running = False

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.consumer_group,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await self.consumer.start()
        self.running = True
        logger.info(f"[{self.__class__.__name__}] Started — consuming {self.topics}")

    async def stop(self):
        self.running = False
        if self.consumer:
            await self.consumer.stop()
        logger.info(f"[{self.__class__.__name__}] Stopped")

    async def run(self):
        await self.start()
        try:
            async for message in self.consumer:
                if not self.running:
                    break
                try:
                    logger.info(
                        f"[{self.__class__.__name__}] Received msg "
                        f"topic={message.topic} key={message.key} offset={message.offset}"
                    )
                    await self.handle_message(message.value)
                except Exception as e:
                    logger.exception(
                        f"[{self.__class__.__name__}] Error handling message: {e}"
                    )
                    await asyncio.sleep(0.5)
        finally:
            await self.stop()

    async def handle_message(self, payload: dict):
        raise NotImplementedError