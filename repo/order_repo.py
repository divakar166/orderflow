from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from models.order import Order
from datetime import datetime


class OrderRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, order: Order) -> Order:
        self.db.add(order)
        await self.db.commit()
        await self.db.refresh(order)
        return order

    async def get_by_id(self, order_id: str) -> Order | None:
        result = await self.db.execute(select(Order).where(Order.order_id == order_id))
        return result.scalar_one_or_none()

    async def update(self, order: Order) -> Order:
        order.updated_at = datetime.utcnow()
        await self.db.commit()
        await self.db.refresh(order)
        return order

    async def list_orders(self, limit: int = 50, offset: int = 0):
        count_result = await self.db.execute(select(func.count()).select_from(Order))
        total = count_result.scalar()
        result = await self.db.execute(
            select(Order).order_by(Order.created_at.desc()).limit(limit).offset(offset)
        )
        return total, result.scalars().all()