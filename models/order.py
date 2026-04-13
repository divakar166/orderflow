import uuid
from datetime import datetime
from sqlalchemy import String, Integer, Float, Enum, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column
from core.database import Base
import enum


class OrderStatus(str, enum.Enum):
    NEW_ORDER = "NEW_ORDER"
    VALIDATING = "VALIDATING"
    VALIDATED = "VALIDATED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    INVENTORY_CHECKING = "INVENTORY_CHECKING"
    INVENTORY_RESERVED = "INVENTORY_RESERVED"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    PAYMENT_PENDING = "PAYMENT_PENDING"
    PAYMENT_SUCCESS = "PAYMENT_SUCCESS"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRY_PENDING = "RETRY_PENDING"


class FailureType(str, enum.Enum):
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INVENTORY_ERROR = "INVENTORY_ERROR"
    PAYMENT_ERROR = "PAYMENT_ERROR"
    SYSTEM_ERROR = "SYSTEM_ERROR"


class Order(Base):
    __tablename__ = "orders"

    order_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    customer_id: Mapped[str] = mapped_column(String(36), nullable=False)
    product: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    total: Mapped[float] = mapped_column(Float, nullable=False)

    current_stage: Mapped[str] = mapped_column(String(50), default="INTAKE")
    order_status: Mapped[str] = mapped_column(
        String(50), default=OrderStatus.NEW_ORDER
    )
    validation_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    inventory_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    payment_status: Mapped[str | None] = mapped_column(String(50), nullable=True)

    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    validated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    inventory_checked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    payment_processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)