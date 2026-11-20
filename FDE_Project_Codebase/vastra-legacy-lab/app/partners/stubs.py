"""Stub partner APIs (the only real APIs in today's estate): courier, payment gateway, SMS and email.

Run: uvicorn app.partners.stubs:app --port 8100
LATENCY_SCALE (env) shrinks delays for tests; PAYMENT_FAIL_RATE (env) sets payment failures.
"""
import os, random, time, math, itertools
import threading
from fastapi import FastAPI, Header
from pydantic import BaseModel

app = FastAPI(title="Vastra partner stubs")
SCALE = float(os.getenv("LATENCY_SCALE", "1.0"))
FAIL = float(os.getenv("PAYMENT_FAIL_RATE", "0.031"))
_charge_ids = itertools.count(1)
CHARGES = []                         # every successful charge, to show duplicates on retry
_charge_lock = threading.Lock()
_idempotent_charges = {}
WAREHOUSE_EVENTS = []
ERP_EVENTS = []


class Charge(BaseModel):
    order_ref: str
    amount: int


class EventDelivery(BaseModel):
    event_id: str
    event_type: str
    payload: dict


@app.get("/courier/promise")
def courier_promise(pincode: str = "560001"):
    # W2.5: 300 ms - 2 s, sometimes much slower
    ms = min(max(random.lognormvariate(math.log(620), 0.51), 300), 2000) if random.random() > 0.017 else random.uniform(2000, 5000)
    time.sleep(ms / 1000 * SCALE)
    return {"pincode": pincode, "delivery_days": random.choice([2, 3, 4]), "cod_available": random.random() < 0.8}


@app.post("/payment/charge")
def payment_charge(c: Charge, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
    if idempotency_key:
        with _charge_lock:
            existing = _idempotent_charges.get(idempotency_key)
            if existing is not None:
                return existing
    time.sleep(random.uniform(0.2, 0.6) * SCALE)
    if random.random() < FAIL:
        return {"status": "failed", "reason": random.choice(["gateway_timeout", "otp_failed", "bank_declined"])}
    with _charge_lock:
        if idempotency_key and idempotency_key in _idempotent_charges:
            return _idempotent_charges[idempotency_key]
        result = {"status": "success", "charge_id": next(_charge_ids)}
        CHARGES.append({"charge_id": result["charge_id"], "order_ref": c.order_ref, "amount": c.amount})
        if idempotency_key:
            _idempotent_charges[idempotency_key] = result
        return result


def _receive_event(events: list, event: EventDelivery):
    if not any(item["event_id"] == event.event_id for item in events):
        events.append(event.model_dump())
    return {"accepted": True, "event_id": event.event_id}


@app.post("/warehouse/events")
def warehouse_event(event: EventDelivery):
    return _receive_event(WAREHOUSE_EVENTS, event)


@app.post("/erp/events")
def erp_event(event: EventDelivery):
    return _receive_event(ERP_EVENTS, event)


@app.post("/notify")
def notify(to: str = "customer", channel: str = "sms"):
    return {"queued": True, "to": to, "channel": channel}
