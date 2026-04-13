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


class InventoryWorker(BaseWorker):
    consumer_group = "inventory-group"
    topics = [settings.KAFKA_TOPIC_PROCESSING]

    async def handle_message(self, payload: dict):
        # Only process messages intended for inventory stage
        stage = payload.get("stage")
        if stage != "INVENTORY":
            return

        order_id = payload.get("order_id")
        if not order_id:
            return

        async with AsyncSessionLocal() as db:
            repo = OrderRepository(db)
            event_svc = EventService(db)

            order = await repo.get_by_id(order_id)
            if not order:
                logger.warning(f"InventoryWorker: order {order_id} not found")
                return

            order.order_status = OrderStatus.INVENTORY_CHECKING
            order.current_stage = "INVENTORY"
            await repo.update(order)

            await event_svc.log(
                order_id=order_id,
                event_type="INVENTORY_CHECK_STARTED",
                event_source="inventory_worker",
            )

            # Simulate inventory check
            in_stock = random.random() < settings.INVENTORY_PASS_RATE

            if in_stock:
                order.order_status = OrderStatus.INVENTORY_RESERVED
                order.inventory_status = "RESERVED"
                order.inventory_checked_at = datetime.utcnow()
                await repo.update(order)

                await event_svc.log(
                    order_id=order_id,
                    event_type="INVENTORY_RESERVED",
                    event_source="inventory_worker",
                    payload={"product": order.product, "quantity": order.quantity},
                )

                await publish_event(
                    topic=settings.KAFKA_TOPIC_PROCESSING,
                    key=order_id,
                    payload={
                        "event_type": "order.inventory_reserved",
                        "order_id": order_id,
                        "stage": "PAYMENT",
                        "product": order.product,
                        "total": order.total,
                        "customer_id": order.customer_id,
                    },
                )
                logger.info(f"InventoryWorker: order {order_id} INVENTORY_RESERVED → forwarded to payment")

            else:
                failure_reason = f"Product '{order.product}' is out of stock (qty requested: {order.quantity})"
                order.order_status = OrderStatus.OUT_OF_STOCK
                order.inventory_status = "OUT_OF_STOCK"
                order.failure_reason = failure_reason
                order.failure_type = FailureType.INVENTORY_ERROR
                await repo.update(order)

                await event_svc.log(
                    order_id=order_id,
                    event_type="INVENTORY_OUT_OF_STOCK",
                    event_source="inventory_worker",
                    payload={"reason": failure_reason},
                )

                await publish_event(
                    topic=settings.KAFKA_TOPIC_FAILED,
                    key=order_id,
                    payload={
                        "event_type": "order.out_of_stock",
                        "order_id": order_id,
                        "reason": failure_reason,
                        "failure_type": FailureType.INVENTORY_ERROR,
                    },
                )
                logger.info(f"InventoryWorker: order {order_id} OUT_OF_STOCK → sent to failed topic")