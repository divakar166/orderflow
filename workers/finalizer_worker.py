import logging
from datetime import datetime
from workers.base_worker import BaseWorker
from core.config import settings
from core.database import AsyncSessionLocal
from repo.order_repo import OrderRepository
from services.event_service import EventService
from models.order import OrderStatus

logger = logging.getLogger(__name__)


class FinalizerWorker(BaseWorker):
    """
    Consumes from both orders.completed and orders.failed.
    Sets the terminal order status (COMPLETED or FAILED) in the DB.
    This is the single place responsible for writing the final state.
    """
    consumer_group = "finalizer-group"
    topics = [settings.KAFKA_TOPIC_COMPLETED, settings.KAFKA_TOPIC_FAILED]

    async def handle_message(self, payload: dict):
        order_id = payload.get("order_id")
        event_type = payload.get("event_type", "")

        if not order_id:
            return

        async with AsyncSessionLocal() as db:
            repo = OrderRepository(db)
            event_svc = EventService(db)

            order = await repo.get_by_id(order_id)
            if not order:
                logger.warning(f"FinalizerWorker: order {order_id} not found")
                return

            # Completed path
            if "payment_success" in event_type or "completed" in event_type:
                order.order_status = OrderStatus.COMPLETED
                order.current_stage = "DONE"
                order.completed_at = datetime.utcnow()
                await repo.update(order)

                await event_svc.log(
                    order_id=order_id,
                    event_type="ORDER_COMPLETED",
                    event_source="finalizer_worker",
                    payload={"total": order.total},
                )
                logger.info(f"FinalizerWorker: order {order_id} → COMPLETED ✓")

            # Failed path
            elif any(x in event_type for x in ["failed", "out_of_stock", "validation_failed"]):
                # Don't overwrite a more specific terminal status already set by a worker
                if order.order_status not in (
                    OrderStatus.COMPLETED,
                    OrderStatus.FAILED,
                    OrderStatus.PAYMENT_FAILED,
                    OrderStatus.VALIDATION_FAILED,
                    OrderStatus.OUT_OF_STOCK,
                ):
                    order.order_status = OrderStatus.FAILED
                    order.current_stage = "DONE"

                order.completed_at = datetime.utcnow()
                await repo.update(order)

                await event_svc.log(
                    order_id=order_id,
                    event_type="ORDER_FAILED",
                    event_source="finalizer_worker",
                    payload={
                        "final_status": order.order_status,
                        "failure_type": order.failure_type,
                        "reason": order.failure_reason,
                    },
                )
                logger.info(
                    f"FinalizerWorker: order {order_id} → FAILED "
                    f"[{order.failure_type}] {order.failure_reason}"
                )