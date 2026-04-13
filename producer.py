"""
producer.py
-----------
Standalone script — uses Faker to generate random orders and POSTs them
to the FastAPI /orders endpoint. Run this separately after the API is up.

Usage:
    python producer.py                  # default: 20 orders, 1s delay
    python producer.py --count 50       # 50 orders
    python producer.py --count 100 --delay 0.3
"""

import asyncio
import argparse
import logging
import random
import httpx
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("producer")

fake = Faker()

API_BASE_URL = "http://localhost:8000"

CATALOG = [
    {"product": "Wireless Noise-Cancelling Headphones", "category": "Electronics", "price_range": (80, 350)},
    {"product": "Mechanical Keyboard", "category": "Electronics", "price_range": (60, 200)},
    {"product": "USB-C Hub 7-in-1", "category": "Electronics", "price_range": (25, 80)},
    {"product": "Running Shoes Pro", "category": "Footwear", "price_range": (60, 180)},
    {"product": "Leather Wallet", "category": "Accessories", "price_range": (20, 90)},
    {"product": "Yoga Mat Premium", "category": "Sports", "price_range": (30, 100)},
    {"product": "Protein Powder 2kg", "category": "Nutrition", "price_range": (35, 70)},
    {"product": "Desk Lamp LED", "category": "Home", "price_range": (25, 80)},
    {"product": "Coffee Maker Pro", "category": "Kitchen", "price_range": (50, 250)},
    {"product": "Bluetooth Speaker", "category": "Electronics", "price_range": (30, 150)},
    {"product": "Gaming Mouse", "category": "Electronics", "price_range": (40, 120)},
    {"product": "Smart Water Bottle", "category": "Sports", "price_range": (20, 60)},
    {"product": "Backpack 30L", "category": "Bags", "price_range": (40, 150)},
    {"product": "Sunglasses Polarized", "category": "Accessories", "price_range": (25, 200)},
    {"product": "Hardcover Notebook", "category": "Stationery", "price_range": (10, 35)},
]


def generate_order_payload() -> dict:
    item = random.choice(CATALOG)
    price = round(random.uniform(*item["price_range"]), 2)
    quantity = random.randint(1, 5)

    return {
        "customer_id": str(fake.uuid4()),
        "product": item["product"],
        "category": item["category"],
        "quantity": quantity,
        "price": price,
    }


async def send_order(client: httpx.AsyncClient, index: int) -> bool:
    payload = generate_order_payload()
    try:
        response = await client.post(f"{API_BASE_URL}/orders/", json=payload)
        if response.status_code == 201:
            data = response.json()
            logger.info(
                f"[{index:>4}] ✓ Created order {data['order_id'][:8]}... "
                f"| {payload['product']} x{payload['quantity']} "
                f"@ ₹{payload['price']} "
                f"= ₹{data['total']}"
            )
            return True
        else:
            logger.error(f"[{index:>4}] ✗ HTTP {response.status_code} — {response.text}")
            return False
    except httpx.ConnectError:
        logger.error(f"[{index:>4}] ✗ Cannot connect to API at {API_BASE_URL}. Is the server running?")
        return False
    except Exception as e:
        logger.error(f"[{index:>4}] ✗ Unexpected error: {e}")
        return False


async def run(count: int, delay: float):
    logger.info(f"Producer starting — sending {count} orders with {delay}s delay each")
    logger.info(f"Target API: {API_BASE_URL}")
    print("-" * 70)

    success = 0
    failed = 0

    async with httpx.AsyncClient(timeout=10.0) as client:
        for i in range(1, count + 1):
            ok = await send_order(client, i)
            if ok:
                success += 1
            else:
                failed += 1
            if i < count:
                await asyncio.sleep(delay)

    print("-" * 70)
    logger.info(f"Done. Sent: {count} | Success: {success} | Failed: {failed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Faker order producer")
    parser.add_argument("--count", type=int, default=20, help="Number of orders to send")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between orders (seconds)")
    args = parser.parse_args()

    asyncio.run(run(count=args.count, delay=args.delay))