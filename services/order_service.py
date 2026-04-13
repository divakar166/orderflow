import uuid
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from models.order import Order, OrderStatus
from schemas.order import CreateOrderRequest
from repo.order_repo import OrderRepository
from services.event_service import EventService
from core.kafka import publish_event
from core.config import settings

logger = logging.getLogger(__name__)


class OrderService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.order_repo = OrderRepository(db)
        self.event_service = EventService(db)

    async def create_order(self, request: CreateOrderRequest) -> Order:
        order = Order(
            order_id=str(uuid.uuid4()),
            customer_id=request.customer_id,
            product=request.product,
            category=request.category,
            quantity=request.quantity,
            price=request.price,
            total=round(request.quantity * request.price, 2),
            order_status=OrderStatus.NEW_ORDER,
            current_stage="INTAKE",
        )

        order = await self.order_repo.create(order)
        logger.info(f"Order created: {order.order_id}")

        # Log intake event
        await self.event_service.log(
            order_id=order.order_id,
            event_type="ORDER_CREATED",
            event_source="api",
            payload={
                "product": order.product,
                "quantity": order.quantity,
                "total": order.total,
            },
        )

        # Publish to Kafka
        await publish_event(
            topic=settings.KAFKA_TOPIC_CREATED,
            key=order.order_id,
            payload={
                "event_type": "order.created",
                "order_id": order.order_id,
                "customer_id": order.customer_id,
                "product": order.product,
                "category": order.category,
                "quantity": order.quantity,
                "price": order.price,
                "total": order.total,
            },
        )

        return order

    async def get_order(self, order_id: str) -> Order | None:
        return await self.order_repo.get_by_id(order_id)

    async def list_orders(self, limit: int = 50, offset: int = 0):
        return await self.order_repo.list_orders(limit=limit, offset=offset)