"""Vastra API v1. The legacy database is accessed only through read-only queries."""
import asyncio
import datetime as dt
import hashlib
import json
import logging
import os
import sqlite3
import uuid
from pathlib import Path
from typing import Literal

import httpx
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app import db as legacy_db
from app.v1 import pricing, store

logger = logging.getLogger("vastra.v1")
PARTNER_BASE_URL = os.getenv("VASTRA_PARTNER_BASE_URL", legacy_db.CFG["partners"]["base_url"])
PARTNER_TRANSPORT: httpx.AsyncBaseTransport | None = None
COURIER_TIMEOUT = 0.8
PAYMENT_TIMEOUT = 3.0
CATALOG_MAX_AGE = 60

app = FastAPI(title="Vastra API v1", version="1.0.0")


@app.middleware("http")
async def request_id_header(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", "")
    if not request_id or len(request_id) > 128 or not all(
        char.isalnum() or char in "-_.:" for char in request_id
    ):
        request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _legacy_product(sku: str) -> dict:
    legacy_uri = Path(legacy_db.DB_PATH).resolve().as_uri() + "?mode=ro"
    try:
        con = sqlite3.connect(legacy_uri, uri=True, timeout=5)
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM products WHERE sku=?", (sku,)).fetchone()
    except sqlite3.Error:
        logger.exception("Could not read the legacy catalogue")
        raise HTTPException(503, "legacy catalogue is unavailable") from None
    finally:
        if "con" in locals():
            con.close()
    if row is None:
        raise HTTPException(404, "unknown sku")
    return dict(row)


def _active_override(sku: str) -> sqlite3.Row | None:
    con = store.connect()
    try:
        return con.execute(
            """SELECT override_id, name, price FROM price_overrides
               WHERE sku=? AND active=1 AND (expires_at IS NULL OR expires_at>?)
               ORDER BY created_at DESC LIMIT 1""",
            (sku, _now()),
        ).fetchone()
    finally:
        con.close()


def _price(product: dict, coupon_pct: int, member: bool) -> dict:
    override = _active_override(product["sku"])
    return {
        "sku": product["sku"],
        "price": pricing.price(
            product, coupon_pct=coupon_pct, member=member,
            override_price=override["price"] if override else None,
        ),
        "policy_version": pricing.POLICY_VERSION,
        "override": (
            {"id": override["override_id"], "name": override["name"]}
            if override else None
        ),
    }


class StockEvent(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    sku: str = Field(min_length=1)
    location_id: str = Field(default="online", min_length=1, max_length=100)
    quantity_delta: int
    source: Literal["pos", "warehouse"]
    occurred_at: dt.datetime | None = None


class PriceOverride(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    sku: str = Field(min_length=1)
    price: int = Field(ge=0)
    actor: str = Field(min_length=1, max_length=100)
    reason: str = Field(min_length=1, max_length=500)
    expires_at: dt.datetime | None = None


class CheckoutInput(BaseModel):
    sku: str = Field(min_length=1)
    qty: int = Field(default=1, ge=1, le=100)
    coupon_pct: int = Field(default=0, ge=0, le=100)
    member: bool = False
    pincode: str = Field(default="560001", min_length=1, max_length=20)
    channel: Literal["web", "mobile", "pos", "contact-centre"] = "web"


@app.get("/v1/health")
def health():
    store.initialize()
    return {"status": "ok", "service": "vastra-api", "version": "v1"}


@app.get("/v1/catalog/{sku}")
def catalog_item(sku: str, request: Request):
    product = _legacy_product(sku)
    body = json.dumps(product, sort_keys=True, separators=(",", ":"), default=str)
    etag = '"' + hashlib.sha256(body.encode("utf-8")).hexdigest() + '"'
    headers = {"ETag": etag, "Cache-Control": f"public, max-age={CATALOG_MAX_AGE}, must-revalidate"}
    if request.headers.get("If-None-Match") == etag:
        return Response(status_code=304, headers=headers)
    return JSONResponse(product, headers=headers)


@app.get("/v1/prices/{sku}")
def get_price(
    sku: str,
    coupon_pct: int = Query(default=0, ge=0, le=100),
    member: bool = False,
):
    return _price(_legacy_product(sku), coupon_pct, member)


@app.post("/v1/pricing/overrides", status_code=201)
def create_price_override(override: PriceOverride):
    _legacy_product(override.sku)
    override_id = str(uuid.uuid4())
    now = _now()
    expires_at = None
    if override.expires_at:
        expiry = override.expires_at
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=dt.timezone.utc)
        expires_at = expiry.astimezone(dt.timezone.utc).isoformat()
    details = json.dumps(override.model_dump(mode="json"), sort_keys=True)
    con = store.connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            """INSERT INTO price_overrides
               (override_id, name, sku, price, actor, reason, created_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (override_id, override.name, override.sku, override.price, override.actor,
             override.reason, now, expires_at),
        )
        con.execute(
            """INSERT INTO price_override_audit (override_id, action, actor, details, at)
               VALUES (?, 'created', ?, ?, ?)""",
            (override_id, override.actor, details, now),
        )
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    return {"override_id": override_id, "status": "active", "policy_version": pricing.POLICY_VERSION}


@app.delete("/v1/pricing/overrides/{override_id}")
def deactivate_price_override(override_id: str, x_actor_id: str = Header(min_length=1)):
    con = store.connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            "SELECT name, sku, price FROM price_overrides WHERE override_id=? AND active=1",
            (override_id,),
        ).fetchone()
        if row is None:
            con.rollback()
            raise HTTPException(404, "active override not found")
        con.execute("UPDATE price_overrides SET active=0 WHERE override_id=?", (override_id,))
        con.execute(
            """INSERT INTO price_override_audit (override_id, action, actor, details, at)
               VALUES (?, 'deactivated', ?, ?, ?)""",
            (override_id, x_actor_id, json.dumps(dict(row), sort_keys=True), _now()),
        )
        con.commit()
    except HTTPException:
        raise
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    return {"override_id": override_id, "status": "inactive"}


@app.get("/v1/stock/{sku}")
def stock_level(sku: str, location_id: str | None = Query(default=None, min_length=1, max_length=100)):
    _legacy_product(sku)
    con = store.connect()
    try:
        if location_id:
            row = con.execute(
                """SELECT SUM(on_hand) AS on_hand, SUM(reserved) AS reserved, MAX(updated_at) AS updated_at
                   FROM stock WHERE sku=? AND location_id=?""",
                (sku, location_id),
            ).fetchone()
        else:
            row = con.execute(
                """SELECT SUM(on_hand) AS on_hand, SUM(reserved) AS reserved, MAX(updated_at) AS updated_at
                   FROM stock WHERE sku=?""",
                (sku,),
            ).fetchone()
    finally:
        con.close()
    if row is None or row["on_hand"] is None:
        raise HTTPException(404, "stock not initialized for sku")
    return {
        "sku": sku,
        "location_id": location_id,
        "on_hand": row["on_hand"],
        "reserved": row["reserved"],
        "available": row["on_hand"] - row["reserved"],
        "updated_at": row["updated_at"],
    }


@app.post("/v1/stock/events", status_code=202)
def record_stock_event(event: StockEvent):
    if event.quantity_delta == 0:
        raise HTTPException(422, "quantity_delta must not be zero")
    _legacy_product(event.sku)
    event_time = event.occurred_at or dt.datetime.now(dt.timezone.utc)
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=dt.timezone.utc)
    occurred_at = event_time.astimezone(dt.timezone.utc).isoformat()
    received_at = _now()
    con = store.connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        previous = con.execute(
            "SELECT sku, location_id, quantity_delta, source FROM stock_events WHERE event_id=?",
            (event.event_id,),
        ).fetchone()
        if previous is not None:
            if (
                previous["sku"], previous["location_id"], previous["quantity_delta"], previous["source"]
            ) != (
                event.sku, event.location_id, event.quantity_delta, event.source
            ):
                raise HTTPException(409, "event_id was already used for a different stock change")
            con.commit()
            return {"event_id": event.event_id, "status": "duplicate"}
        con.execute(
            """INSERT OR IGNORE INTO stock (sku, location_id, on_hand, updated_at)
               VALUES (?, ?, 0, ?)""",
            (event.sku, event.location_id, received_at),
        )
        stock = con.execute(
            "SELECT on_hand, reserved FROM stock WHERE sku=? AND location_id=?",
            (event.sku, event.location_id),
        ).fetchone()
        updated = stock["on_hand"] + event.quantity_delta
        if updated < stock["reserved"]:
            raise HTTPException(409, "stock event would reduce on-hand below reserved quantity")
        con.execute(
            "UPDATE stock SET on_hand=?, updated_at=? WHERE sku=? AND location_id=?",
            (updated, received_at, event.sku, event.location_id),
        )
        con.execute(
            """INSERT INTO stock_events
               (event_id, sku, location_id, quantity_delta, source, occurred_at, received_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                event.event_id, event.sku, event.location_id, event.quantity_delta,
                event.source, occurred_at, received_at,
            ),
        )
        _enqueue(
            con,
            f"stock:{event.event_id}",
            "inventory.changed",
            {
                "event_id": event.event_id,
                "sku": event.sku,
                "location_id": event.location_id,
                "quantity_delta": event.quantity_delta,
                "source": event.source,
                "occurred_at": occurred_at,
            },
            destinations=("warehouse",),
        )
        con.commit()
    except HTTPException:
        con.rollback()
        raise
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    return {"event_id": event.event_id, "status": "applied"}


def _partner_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=PARTNER_BASE_URL,
        transport=PARTNER_TRANSPORT,
        timeout=None,
    )


def _request_hash(payload: CheckoutInput) -> str:
    canonical = json.dumps(payload.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _replay_or_start(
    payload: CheckoutInput, key: str, request_hash: str
) -> tuple[str, JSONResponse | None]:
    con = store.connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        existing = con.execute(
            "SELECT * FROM idempotency WHERE idempotency_key=?", (key,)
        ).fetchone()
        if existing:
            if existing["request_hash"] != request_hash:
                raise HTTPException(409, "Idempotency-Key was already used with a different request")
            if existing["status"] == "completed":
                con.commit()
                return "", JSONResponse(
                    json.loads(existing["response_json"]),
                    status_code=existing["response_status"],
                    headers={"Idempotency-Replayed": "true"},
                )
            if existing["status"] == "processing":
                raise HTTPException(409, "checkout with this Idempotency-Key is already in progress")
            reservation = con.execute(
                "SELECT order_ref FROM reservations WHERE order_ref=? AND status='pending'",
                (existing["response_json"],),
            ).fetchone()
            if existing["status"] == "retryable" and reservation:
                con.execute(
                    "UPDATE idempotency SET status='processing', updated_at=? WHERE idempotency_key=?",
                    (_now(), key),
                )
                con.commit()
                return existing["response_json"], None
            raise HTTPException(409, "checkout for this Idempotency-Key cannot be resumed")

        stock = con.execute(
            "SELECT on_hand, reserved FROM stock WHERE sku=? AND location_id='online'",
            (payload.sku,),
        ).fetchone()
        if stock is None or stock["on_hand"] - stock["reserved"] < payload.qty:
            raise HTTPException(409, "out of stock")
        order_ref = str(uuid.uuid4())
        now = _now()
        con.execute(
            """INSERT INTO idempotency
               (idempotency_key, request_hash, status, response_json, updated_at)
               VALUES (?, ?, 'processing', ?, ?)""",
            (key, request_hash, order_ref, now),
        )
        con.execute(
            """UPDATE stock SET reserved=reserved+?, updated_at=?
               WHERE sku=? AND location_id='online'""",
            (payload.qty, now, payload.sku),
        )
        con.execute(
            """INSERT INTO reservations
               (order_ref, sku, location_id, qty, status, created_at)
               VALUES (?, ?, 'online', ?, 'pending', ?)""",
            (order_ref, payload.sku, payload.qty, now),
        )
        con.commit()
        return order_ref, None
    except HTTPException:
        con.rollback()
        raise
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def _release_reservation(order_ref: str, key: str, status: str, response_status: int, body: dict) -> None:
    con = store.connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        reservation = con.execute(
            "SELECT sku, location_id, qty FROM reservations WHERE order_ref=? AND status='pending'",
            (order_ref,),
        ).fetchone()
        if reservation:
            con.execute(
                "UPDATE stock SET reserved=reserved-?, updated_at=? WHERE sku=? AND location_id=?",
                (reservation["qty"], _now(), reservation["sku"], reservation["location_id"]),
            )
            con.execute("UPDATE reservations SET status='released' WHERE order_ref=?", (order_ref,))
        con.execute(
            """UPDATE idempotency SET status=?, response_status=?, response_json=?, updated_at=?
               WHERE idempotency_key=?""",
            (status, response_status, json.dumps(body), _now(), key),
        )
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def _enqueue(
    con: sqlite3.Connection,
    event_id: str,
    event_type: str,
    body: dict,
    destinations: tuple[str, ...] = ("warehouse", "erp"),
) -> None:
    now = _now()
    con.execute(
        "INSERT INTO outbox (event_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?)",
        (event_id, event_type, json.dumps(body, sort_keys=True), now),
    )
    for destination in destinations:
        con.execute(
            "INSERT INTO outbox_deliveries (event_id, destination) VALUES (?, ?)",
            (event_id, destination),
        )


@app.post("/v1/checkout")
async def checkout(
    payload: CheckoutInput,
    request: Request,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
):
    product = _legacy_product(payload.sku)
    unit_price = _price(product, payload.coupon_pct, payload.member)["price"]
    request_hash = _request_hash(payload)
    order_ref, replay = _replay_or_start(payload, idempotency_key, request_hash)
    if replay is not None:
        return replay
    request_id = request.state.request_id

    async def courier(client: httpx.AsyncClient) -> tuple[dict, bool]:
        try:
            response = await asyncio.wait_for(
                client.get(
                    "/courier/promise",
                    params={"pincode": payload.pincode},
                    headers={"X-Request-ID": request_id},
                ),
                timeout=COURIER_TIMEOUT,
            )
            response.raise_for_status()
            return response.json(), False
        except (httpx.HTTPError, asyncio.TimeoutError):
            logger.warning("Courier timeout/error; using fallback delivery estimate")
            return {"delivery_days": 3, "cod_available": False}, True

    async def payment(client: httpx.AsyncClient) -> dict:
        response = await asyncio.wait_for(
            client.post(
                "/payment/charge",
                json={"order_ref": order_ref, "amount": unit_price * payload.qty},
                headers={
                    "Idempotency-Key": idempotency_key,
                    "X-Request-ID": request_id,
                },
                timeout=PAYMENT_TIMEOUT,
            ),
            timeout=PAYMENT_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    try:
        async with _partner_client() as client:
            promise, payment_result = await asyncio.gather(courier(client), payment(client))
    except (httpx.HTTPError, asyncio.TimeoutError):
        logger.exception("Payment partner failed for order %s", order_ref)
        body = {"detail": "payment service unavailable; retry with the same Idempotency-Key"}
        con = store.connect()
        try:
            con.execute(
                "UPDATE idempotency SET status='retryable', response_json=?, updated_at=? WHERE idempotency_key=?",
                (order_ref, _now(), idempotency_key),
            )
            con.commit()
        finally:
            con.close()
        return JSONResponse(body, status_code=503)

    if payment_result.get("status") != "success":
        body = {"order_ref": order_ref, "status": "payment_failed"}
        _release_reservation(order_ref, idempotency_key, "completed", 402, body)
        return JSONResponse(body, status_code=402)

    created_at = _now()
    body = {
        "order_ref": order_ref,
        "status": "paid",
        "total": unit_price * payload.qty,
        "delivery_days": promise[0]["delivery_days"],
        "courier_fallback": promise[1],
        "policy_version": pricing.POLICY_VERSION,
    }
    con = store.connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        reservation = con.execute(
            """SELECT sku, location_id, qty FROM reservations
               WHERE order_ref=? AND status='pending'""",
            (order_ref,),
        ).fetchone()
        if reservation is None:
            raise RuntimeError(f"Reservation for order {order_ref} is missing")
        con.execute(
            """UPDATE stock SET on_hand=on_hand-?, reserved=reserved-?, updated_at=?
               WHERE sku=? AND location_id=?""",
            (
                reservation["qty"], reservation["qty"], created_at,
                reservation["sku"], reservation["location_id"],
            ),
        )
        con.execute("UPDATE reservations SET status='committed' WHERE order_ref=?", (order_ref,))
        con.execute(
            """INSERT INTO orders (order_ref, sku, qty, total, channel, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'paid', ?)""",
            (order_ref, payload.sku, payload.qty, unit_price * payload.qty, payload.channel, created_at),
        )
        _enqueue(con, order_ref, "order.paid", body | {
            "sku": payload.sku, "qty": payload.qty, "channel": payload.channel, "created_at": created_at
        })
        con.execute(
            """UPDATE idempotency SET status='completed', response_status=200,
               response_json=?, updated_at=? WHERE idempotency_key=?""",
            (json.dumps(body), created_at, idempotency_key),
        )
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    return body
