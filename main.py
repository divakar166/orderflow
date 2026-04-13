import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from core.database import create_tables
from core.kafka import stop_producer
from api.orders import router as orders_router
from workers.validation_worker import ValidationWorker
from workers.inventory_worker import InventoryWorker
from workers.payment_worker import PaymentWorker
from workers.finalizer_worker import FinalizerWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

_worker_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Creating DB tables...")
    await create_tables()

    logger.info("Starting Kafka workers...")
    workers = [
        ValidationWorker(),
        InventoryWorker(),
        PaymentWorker(),
        FinalizerWorker(),
    ]
    for worker in workers:
        task = asyncio.create_task(worker.run(), name=worker.__class__.__name__)
        _worker_tasks.append(task)

    logger.info("All workers started. API ready.")
    yield

    logger.info("Shutting down workers...")
    for task in _worker_tasks:
        task.cancel()
    await asyncio.gather(*_worker_tasks, return_exceptions=True)

    await stop_producer()
    logger.info("Shutdown complete.")


app = FastAPI(
    title="Order Processing System",
    description="Event-driven order pipeline: FastAPI + Kafka + PostgreSQL",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(orders_router)


@app.get("/health")
async def health():
    return {"status": "ok"}