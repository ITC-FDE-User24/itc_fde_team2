"""POS adapter; communicates with API v1 through the NGINX gateway only."""
import os

import httpx

GATEWAY_URL = os.getenv("VASTRA_GATEWAY_URL", "http://127.0.0.1:8080")


def price(sku: str, coupon_pct: int = 0, member: bool = False) -> dict:
    response = httpx.get(
        f"{GATEWAY_URL}/v1/prices/{sku}",
        params={"coupon_pct": coupon_pct, "member": member},
        timeout=5.0,
    )
    response.raise_for_status()
    return response.json()


def post_stock_event(event_id: str, sku: str, store_id: str, quantity_delta: int) -> dict:
    response = httpx.post(
        f"{GATEWAY_URL}/v1/stock/events",
        json={
            "event_id": event_id,
            "sku": sku,
            "location_id": store_id,
            "quantity_delta": quantity_delta,
            "source": "pos",
        },
        timeout=5.0,
    )
    response.raise_for_status()
    return response.json()
