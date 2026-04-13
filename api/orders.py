from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from core.database import get_db
from schemas.order import CreateOrderRequest, OrderResponse, OrderListResponse
from services.order_service import OrderService

router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("/", response_model=OrderResponse, status_code=201)
async def create_order(
    request: CreateOrderRequest,
    db: AsyncSession = Depends(get_db),
):
    service = OrderService(db)
    order = await service.create_order(request)
    return order


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: str,
    db: AsyncSession = Depends(get_db),
):
    service = OrderService(db)
    order = await service.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@router.get("/", response_model=OrderListResponse)
async def list_orders(
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    service = OrderService(db)
    total, orders = await service.list_orders(limit=limit, offset=offset)
    return OrderListResponse(total=total, orders=orders)