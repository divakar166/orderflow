from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from models.event import OrderEvent


class EventRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, event: OrderEvent) -> OrderEvent:
        self.db.add(event)
        await self.db.commit()
        await self.db.refresh(event)
        return event

    async def get_by_order_id(self, order_id: str) -> list[OrderEvent]:
        result = await self.db.execute(
            select(OrderEvent)
            .where(OrderEvent.order_id == order_id)
            .order_by(OrderEvent.created_at.asc())
        )
        return result.scalars().all()