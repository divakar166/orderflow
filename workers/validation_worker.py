import random
import logging
from datetime import datetime
from workers.base_worker import BaseWorker
from core.config import settings
from core.database import AsyncSessionLocal
from core.kafka import publish_event
from repo.order_repo import OrderRepository
from services.event_service import EventService
from models.order import OrderStatus, FailureType

logger = logging.getLogger(__name__)


class ValidationWorker(BaseWorker):
    consumer_group = "validation-group"
    topics = [settings.KAFKA_TOPIC_CREATED]

    async def handle_message(self, payload: dict):
        order_id = payload.get("order_id")
        if not order_id:
            logger.warning("ValidationWorker: missing order_id in payload")
            return

        async with AsyncSessionLocal() as db:
            repo = OrderRepository(db)
            event_svc = EventService(db)

            order = await repo.get_by_id(order_id)
            if not order:
                logger.warning(f"ValidationWorker: order {order_id} not found")
                return

            # Mark as VALIDATING
            order.order_status = OrderStatus.VALIDATING
            order.current_stage = "VALIDATION"
            await repo.update(order)

            await event_svc.log(
                order_id=order_id,
                event_type="VALIDATION_STARTED",
                event_source="validation_worker",
            )

            # Simulate validation logic
            passed = random.random() < settings.VALIDATION_PASS_RATE

            if passed:
                order.order_status = OrderStatus.VALIDATED
                order.validation_status = "PASSED"
                order.validated_at = datetime.utcnow()
                await repo.update(order)

                await event_svc.log(
                    order_id=order_id,
                    event_type="VALIDATION_PASSED",
                    event_source="validation_worker",
                    payload={"quantity": order.quantity, "price": order.price},
                )

                # Forward to processing topic
                await publish_event(
                    topic=settings.KAFKA_TOPIC_PROCESSING,
                    key=order_id,
                    payload={
                        "event_type": "order.validated",
                        "order_id": order_id,
                        "stage": "INVENTORY",
                        "product": order.product,
                        "quantity": order.quantity,
                        "category": order.category,
                    },
                )
                logger.info(f"ValidationWorker: order {order_id} VALIDATED → forwarded to processing")

            else:
                failure_reason = "Order failed validation: invalid product data or limits exceeded"
                order.order_status = OrderStatus.VALIDATION_FAILED
                order.validation_status = "FAILED"
                order.failure_reason = failure_reason
                order.failure_type = FailureType.VALIDATION_ERROR
                await repo.update(order)

                await event_svc.log(
                    order_id=order_id,
                    event_type="VALIDATION_FAILED",
                    event_source="validation_worker",
                    payload={"reason": failure_reason},
                )

                await publish_event(
                    topic=settings.KAFKA_TOPIC_FAILED,
                    key=order_id,
                    payload={
                        "event_type": "order.validation_failed",
                        "order_id": order_id,
                        "reason": failure_reason,
                        "failure_type": FailureType.VALIDATION_ERROR,
                    },
                )
                logger.info(f"ValidationWorker: order {order_id} VALIDATION_FAILED → sent to failed topic")