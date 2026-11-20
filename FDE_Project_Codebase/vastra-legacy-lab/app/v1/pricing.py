"""Single v1 pricing policy shared by all channels: vastra-2026-11-v1."""
import datetime as dt
from typing import Any

POLICY_VERSION = "vastra-2026-11-v1"


def _utc(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def price(
    product: dict[str, Any],
    coupon_pct: int = 0,
    member: bool = False,
    now: dt.datetime | None = None,
    override_price: int | None = None,
) -> int:
    if not 0 <= coupon_pct <= 100:
        raise ValueError("coupon_pct must be between 0 and 100")
    if override_price is not None:
        return override_price

    now = now or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    unit = product["base_price"]
    sale_end = product.get("sale_ends_utc")
    if product.get("sale_price") is not None and sale_end:
        if now.astimezone(dt.timezone.utc) < _utc(sale_end):
            unit = product["sale_price"]
    if coupon_pct:
        unit = unit * (100 - coupon_pct) / 100
    if member:
        unit = unit * (100 - product["member_discount_pct"]) / 100
    return round(unit)
