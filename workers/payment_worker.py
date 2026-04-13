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


class PaymentWorker(BaseWorker):
    consumer_group = "payment-group"
    topics = [settings.KAFKA_TOPIC_PROCESSING]

    async def handle_message(self, payload: dict):
        stage = payload.get("stage")
        if stage != "PAYMENT":
            return

        order_id = payload.get("order_id")
        if not order_id:
            return

        async with AsyncSessionLocal() as db:
            repo = OrderRepository(db)
            event_svc = EventService(db)

            order = await repo.get_by_id(order_id)
            if not order:
                logger.warning(f"PaymentWorker: order {order_id} not found")
                return

            order.order_status = OrderStatus.PAYMENT_PENDING
            order.current_stage = "PAYMENT"
            await repo.update(order)

            await event_svc.log(
                order_id=order_id,
                event_type="PAYMENT_INITIATED",
                event_source="payment_worker",
                payload={"total": order.total},
            )

            # Simulate payment gateway
            paid = random.random() < settings.PAYMENT_PASS_RATE

            if paid:
                order.order_status = OrderStatus.PAYMENT_SUCCESS
                order.payment_status = "SUCCESS"
                order.payment_processed_at = datetime.utcnow()
                await repo.update(order)

                await event_svc.log(
                    order_id=order_id,
                    event_type="PAYMENT_SUCCESS",
                    event_source="payment_worker",
                    payload={"total": order.total, "customer_id": order.customer_id},
                )

                await publish_event(
                    topic=settings.KAFKA_TOPIC_COMPLETED,
                    key=order_id,
                    payload={
                        "event_type": "order.payment_success",
                        "order_id": order_id,
                        "customer_id": order.customer_id,
                        "total": order.total,
                    },
                )
                logger.info(f"PaymentWorker: order {order_id} PAYMENT_SUCCESS → forwarded to completed")

            else:
                # Check retry count
                order.retry_count += 1

                if order.retry_count < settings.MAX_RETRY_COUNT:
                    failure_reason = f"Payment gateway declined transaction (attempt {order.retry_count})"
                    order.order_status = OrderStatus.RETRY_PENDING
                    order.payment_status = "RETRY_PENDING"
                    order.failure_reason = failure_reason
                    await repo.update(order)

                    await event_svc.log(
                        order_id=order_id,
                        event_type="PAYMENT_RETRY_SCHEDULED",
                        event_source="payment_worker",
                        payload={"attempt": order.retry_count, "reason": failure_reason},
                    )

                    # Re-publish to processing for retry
                    await publish_event(
                        topic=settings.KAFKA_TOPIC_PROCESSING,
                        key=order_id,
                        payload={
                            "event_type": "order.payment_retry",
                            "order_id": order_id,
                            "stage": "PAYMENT",
                            "total": order.total,
                            "customer_id": order.customer_id,
                            "retry_count": order.retry_count,
                        },
                    )
                    logger.info(
                        f"PaymentWorker: order {order_id} RETRY_PENDING "
                        f"(attempt {order.retry_count}/{settings.MAX_RETRY_COUNT})"
                    )

                else:
                    failure_reason = f"Payment failed after {order.retry_count} attempts — releasing inventory"
                    order.order_status = OrderStatus.PAYMENT_FAILED
                    order.payment_status = "FAILED"
                    order.failure_reason = failure_reason
                    order.failure_type = FailureType.PAYMENT_ERROR
                    # Release inventory reservation
                    order.inventory_status = "RELEASED"
                    await repo.update(order)

                    await event_svc.log(
                        order_id=order_id,
                        event_type="PAYMENT_FAILED_FINAL",
                        event_source="payment_worker",
                        payload={
                            "reason": failure_reason,
                            "attempts": order.retry_count,
                            "inventory_action": "RELEASED",
                        },
                    )

                    await publish_event(
                        topic=settings.KAFKA_TOPIC_FAILED,
                        key=order_id,
                        payload={
                            "event_type": "order.payment_failed",
                            "order_id": order_id,
                            "reason": failure_reason,
                            "failure_type": FailureType.PAYMENT_ERROR,
                        },
                    )
                    logger.info(
                        f"PaymentWorker: order {order_id} PAYMENT_FAILED (exhausted retries) → inventory released"
                    )