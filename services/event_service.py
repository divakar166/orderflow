from sqlalchemy.ext.asyncio import AsyncSession
from models.event import OrderEvent
from repo.event_repo import EventRepository


class EventService:
    def __init__(self, db: AsyncSession):
        self.repo = EventRepository(db)

    async def log(
        self,
        order_id: str,
        event_type: str,
        event_source: str,
        payload: dict | None = None,
    ) -> OrderEvent:
        event = OrderEvent(
            order_id=order_id,
            event_type=event_type,
            event_source=event_source,
            event_payload=payload or {},
        )
        return await self.repo.create(event)