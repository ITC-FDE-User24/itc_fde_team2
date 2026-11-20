"""Persistence for v1-owned stock, orders, idempotency, audit, and outbox data."""
import datetime as dt
import os
import sqlite3
import threading
from pathlib import Path
from app import db as legacy_db

DB_PATH = os.getenv("VASTRA_V1_DB_PATH", os.path.join(legacy_db.ROOT, "data", "vastra_v1.db"))
_init_lock = threading.Lock()
_initialized_path: str | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock (
    sku TEXT NOT NULL,
    location_id TEXT NOT NULL DEFAULT 'online',
    on_hand INTEGER NOT NULL CHECK (on_hand >= 0),
    reserved INTEGER NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    updated_at TEXT NOT NULL,
    PRIMARY KEY (sku, location_id)
);
CREATE TABLE IF NOT EXISTS stock_events (
    event_id TEXT PRIMARY KEY,
    sku TEXT NOT NULL,
    location_id TEXT NOT NULL,
    quantity_delta INTEGER NOT NULL,
    source TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reservations (
    order_ref TEXT PRIMARY KEY,
    sku TEXT NOT NULL,
    location_id TEXT NOT NULL DEFAULT 'online',
    qty INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    response_status INTEGER,
    response_json TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_ref TEXT PRIMARY KEY,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    total INTEGER NOT NULL,
    channel TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS price_overrides (
    override_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    sku TEXT NOT NULL,
    price INTEGER NOT NULL CHECK (price >= 0),
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS price_override_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    override_id TEXT NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    details TEXT NOT NULL,
    at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_deliveries (
    event_id TEXT NOT NULL REFERENCES outbox(event_id),
    destination TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    delivered_at TEXT,
    last_error TEXT,
    PRIMARY KEY (event_id, destination)
);
"""


def _open(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=5)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    return con


def _seed_stock(con: sqlite3.Connection) -> None:
    legacy_uri = Path(legacy_db.DB_PATH).resolve().as_uri() + "?mode=ro"
    try:
        legacy = sqlite3.connect(legacy_uri, uri=True, timeout=5)
        legacy.row_factory = sqlite3.Row
        rows = legacy.execute("SELECT sku, qty FROM stock_copy").fetchall()
    except sqlite3.Error:
        raise RuntimeError(
            "The legacy database must be initialized with app.db before starting API v1"
        ) from None
    finally:
        if "legacy" in locals():
            legacy.close()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    con.executemany(
        """INSERT OR IGNORE INTO stock (sku, location_id, on_hand, updated_at)
           VALUES (?, 'online', ?, ?)""",
        [(row["sku"], row["qty"], now) for row in rows],
    )


def initialize() -> None:
    global _initialized_path
    path = os.path.abspath(DB_PATH)
    if _initialized_path == path and os.path.exists(path):
        return
    with _init_lock:
        if _initialized_path == path and os.path.exists(path):
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        con = _open(path)
        try:
            con.executescript(SCHEMA)
            _seed_stock(con)
            con.commit()
        finally:
            con.close()
        _initialized_path = path


def connect() -> sqlite3.Connection:
    initialize()
    return _open(os.path.abspath(DB_PATH))
