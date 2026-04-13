from pydantic import BaseModel, Field
from datetime import datetime


class CreateOrderRequest(BaseModel):
    customer_id: str
    product: str
    category: str
    quantity: int = Field(gt=0)
    price: float = Field(gt=0)


class OrderResponse(BaseModel):
    order_id: str
    customer_id: str
    product: str
    category: str
    quantity: int
    price: float
    total: float
    order_status: str
    current_stage: str
    validation_status: str | None
    inventory_status: str | None
    payment_status: str | None
    failure_reason: str | None
    failure_type: str | None
    retry_count: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class OrderListResponse(BaseModel):
    total: int
    orders: list[OrderResponse]