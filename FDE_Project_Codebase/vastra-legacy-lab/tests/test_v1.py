import asyncio
import json
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from fastapi.testclient import TestClient
from httpx import Request, Response

from app import db
from app.partners import stubs
from app.v1 import api, outbox, store


@pytest.fixture
def client(tmp_path, monkeypatch):
    legacy_path = str(tmp_path / "legacy.db")
    v1_path = str(tmp_path / "v1.db")
    monkeypatch.setattr(db, "DB_PATH", legacy_path)
    monkeypatch.setattr(store, "DB_PATH", v1_path)
    db.init_and_seed(20)
    legacy = db.connect()
    legacy.execute("UPDATE stock_copy SET qty=10 WHERE sku='SKU00003'")
    legacy.commit()
    legacy.close()
    monkeypatch.setattr(api, "PARTNER_BASE_URL", "http://partners.test")
    monkeypatch.setattr(api, "PARTNER_TRANSPORT", httpx.ASGITransport(app=stubs.app))
    monkeypatch.setattr(stubs, "FAIL", 0)
    monkeypatch.setattr(stubs, "SCALE", 0.01)
    return TestClient(api.app)


def test_catalog_etag_and_pricing_policy(client):
    first = client.get("/v1/catalog/SKU00003")
    assert first.status_code == 200
    assert first.headers["cache-control"].startswith("public, max-age=")
    second = client.get(
        "/v1/catalog/SKU00003",
        headers={"If-None-Match": first.headers["etag"]},
    )
    assert second.status_code == 304

    price = client.get("/v1/prices/SKU00003?coupon_pct=10&member=true")
    assert price.status_code == 200
    assert price.json()["policy_version"] == "vastra-2026-11-v1"


def test_price_override_is_named_and_audited(client):
    response = client.post(
        "/v1/pricing/overrides",
        json={
            "name": "Diwali display",
            "sku": "SKU00003",
            "price": 999,
            "actor": "manager-42",
            "reason": "Approved promotion",
        },
    )
    assert response.status_code == 201
    override_id = response.json()["override_id"]
    assert client.get("/v1/prices/SKU00003").json()["price"] == 999

    removed = client.delete(
        f"/v1/pricing/overrides/{override_id}",
        headers={"X-Actor-ID": "manager-42"},
    )
    assert removed.status_code == 200
    con = store.connect()
    try:
        rows = con.execute(
            "SELECT action, actor FROM price_override_audit ORDER BY audit_id"
        ).fetchall()
        assert [(row["action"], row["actor"]) for row in rows] == [
            ("created", "manager-42"),
            ("deactivated", "manager-42"),
        ]
    finally:
        con.close()


def test_pos_stock_events_are_idempotent_and_guard_available_stock(client):
    event = {
        "event_id": "pos-transaction-1",
        "sku": "SKU00003",
        "quantity_delta": 4,
        "source": "pos",
    }
    first = client.post("/v1/stock/events", json=event)
    assert first.status_code == 202
    assert first.json()["status"] == "applied"
    assert client.post("/v1/stock/events", json=event).json()["status"] == "duplicate"
    conflict = client.post(
        "/v1/stock/events",
        json={**event, "quantity_delta": 5},
    )
    assert conflict.status_code == 409
    assert client.get("/v1/stock/SKU00003").json()["on_hand"] == 14


def test_store_stock_is_separate_from_online_stock(client):
    response = client.post(
        "/v1/stock/events",
        json={
            "event_id": "store-042-receipt-1",
            "sku": "SKU00003",
            "location_id": "store-042",
            "quantity_delta": 12,
            "source": "pos",
        },
    )
    assert response.status_code == 202
    store_level = client.get("/v1/stock/SKU00003?location_id=store-042").json()
    assert store_level["on_hand"] == 12
    assert client.get("/v1/stock/SKU00003").json()["on_hand"] == 22


def test_checkout_is_idempotent_and_emits_outbox_events(client):
    payload = {"sku": "SKU00003", "qty": 2, "coupon_pct": 10, "member": True}
    before_charges = len(stubs.CHARGES)
    first = client.post(
        "/v1/checkout",
        json=payload,
        headers={"Idempotency-Key": "checkout-order-1"},
    )
    assert first.status_code == 200
    assert first.json()["status"] == "paid"
    assert first.json()["courier_fallback"] is False
    replay = client.post(
        "/v1/checkout",
        json=payload,
        headers={"Idempotency-Key": "checkout-order-1"},
    )
    assert replay.status_code == 200
    assert replay.headers["idempotency-replayed"] == "true"
    assert replay.json() == first.json()
    assert len(stubs.CHARGES) - before_charges == 1
    conflict = client.post(
        "/v1/checkout",
        json={**payload, "qty": 1},
        headers={"Idempotency-Key": "checkout-order-1"},
    )
    assert conflict.status_code == 409
    assert client.get("/v1/stock/SKU00003").json()["on_hand"] == 8

    con = store.connect()
    try:
        event = con.execute(
            "SELECT event_type, payload_json FROM outbox WHERE event_id=?",
            (first.json()["order_ref"],),
        ).fetchone()
        assert event["event_type"] == "order.paid"
        assert json.loads(event["payload_json"])["channel"] == "web"
    finally:
        con.close()

    published = asyncio.run(
        outbox.publish_pending(
            base_url="http://partners.test",
            transport=httpx.ASGITransport(app=stubs.app),
        )
    )
    assert published == 2
    assert any(e["event_id"] == first.json()["order_ref"] for e in stubs.WAREHOUSE_EVENTS)
    assert any(e["event_id"] == first.json()["order_ref"] for e in stubs.ERP_EVENTS)


def test_checkout_partner_calls_run_in_parallel_and_forward_request_id(client, monkeypatch):
    class TimedTransport(httpx.AsyncBaseTransport):
        active = 0
        peak_active = 0
        request_ids = []

        async def handle_async_request(self, request: Request) -> Response:
            self.active += 1
            self.peak_active = max(self.peak_active, self.active)
            self.request_ids.append(request.headers.get("x-request-id"))
            try:
                await asyncio.sleep(0.03)
                payload = (
                    {"delivery_days": 2, "cod_available": True}
                    if request.url.path == "/courier/promise"
                    else {"status": "success", "charge_id": 1}
                )
                return Response(200, json=payload, request=request)
            finally:
                self.active -= 1

    transport = TimedTransport()
    monkeypatch.setattr(api, "PARTNER_BASE_URL", "http://partners.test")
    monkeypatch.setattr(api, "PARTNER_TRANSPORT", transport)

    response = client.post(
        "/v1/checkout",
        json={"sku": "SKU00003"},
        headers={
            "Idempotency-Key": "parallel-checkout-a",
            "X-Request-ID": "request-parallel-a",
        },
    )
    assert response.status_code == 200
    assert transport.peak_active == 2
    assert transport.request_ids == ["request-parallel-a", "request-parallel-a"]


def test_stock_reservation_is_atomic_for_concurrent_checkouts(client):
    con = store.connect()
    try:
        con.execute(
            "UPDATE stock SET on_hand=1, reserved=0 WHERE sku='SKU00003' AND location_id='online'"
        )
        con.commit()
    finally:
        con.close()

    def buy(key: str):
        return client.post(
            "/v1/checkout",
            json={"sku": "SKU00003"},
            headers={"Idempotency-Key": key},
        ).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses = list(pool.map(buy, ("parallel-checkout-a", "parallel-checkout-b")))
    assert sorted(statuses) == [200, 409]
    assert client.get("/v1/stock/SKU00003").json()["available"] == 0


def test_payment_timeout_can_retry_with_same_idempotency_key(client, monkeypatch):
    class RetryTransport(httpx.AsyncBaseTransport):
        payment_calls = 0

        async def handle_async_request(self, request: Request) -> Response:
            if request.url.path == "/payment/charge":
                self.payment_calls += 1
                if self.payment_calls == 1:
                    raise httpx.ReadTimeout("simulated lost payment response")
                return Response(200, json={"status": "success", "charge_id": 1}, request=request)
            return Response(200, json={"delivery_days": 2}, request=request)

    transport = RetryTransport()
    monkeypatch.setattr(api, "PARTNER_BASE_URL", "http://partners.test")
    monkeypatch.setattr(api, "PARTNER_TRANSPORT", transport)
    headers = {"Idempotency-Key": "retry-after-payment-timeout"}
    first = client.post("/v1/checkout", json={"sku": "SKU00003"}, headers=headers)
    assert first.status_code == 503
    retry = client.post("/v1/checkout", json={"sku": "SKU00003"}, headers=headers)
    assert retry.status_code == 200
    assert retry.json()["status"] == "paid"
    assert transport.payment_calls == 2
    assert client.get("/v1/stock/SKU00003").json()["on_hand"] == 9


def test_courier_timeout_uses_documented_fallback(client, monkeypatch):
    monkeypatch.setattr(api, "COURIER_TIMEOUT", 0.000001)
    response = client.post(
        "/v1/checkout",
        json={"sku": "SKU00003"},
        headers={"Idempotency-Key": "checkout-courier-fallback"},
    )
    assert response.status_code == 200
    assert response.json()["courier_fallback"] is True
    assert response.json()["delivery_days"] == 3
