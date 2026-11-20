"""Publish committed v1 events to warehouse and ERP consumers."""
import asyncio
import datetime as dt
import json
import logging

import httpx

from app import db as legacy_db
from app.v1 import store

logger = logging.getLogger("vastra.v1.outbox")
DESTINATIONS = {
    "warehouse": "/warehouse/events",
    "erp": "/erp/events",
}


async def publish_pending(
    base_url: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    limit: int = 100,
) -> int:
    store.initialize()
    con = store.connect()
    try:
        rows = con.execute(
            """SELECT o.event_id, o.event_type, o.payload_json, d.destination
               FROM outbox o JOIN outbox_deliveries d ON d.event_id=o.event_id
               WHERE d.delivered_at IS NULL
               ORDER BY o.created_at, o.event_id LIMIT ?""",
            (limit,),
        ).fetchall()
    finally:
        con.close()

    url = base_url or legacy_db.CFG["partners"]["base_url"]
    delivered = 0
    failures: list[str] = []
    async with httpx.AsyncClient(base_url=url, transport=transport, timeout=3.0) as client:
        for row in rows:
            destination = row["destination"]
            try:
                response = await client.post(
                    DESTINATIONS[destination],
                    json={
                        "event_id": row["event_id"],
                        "event_type": row["event_type"],
                        "payload": json.loads(row["payload_json"]),
                    },
                )
                response.raise_for_status()
            except httpx.HTTPError as error:
                con = store.connect()
                try:
                    con.execute(
                        """UPDATE outbox_deliveries SET attempts=attempts+1, last_error=?
                           WHERE event_id=? AND destination=?""",
                        (str(error), row["event_id"], destination),
                    )
                    con.commit()
                finally:
                    con.close()
                logger.exception("Outbox delivery failed: %s to %s", row["event_id"], destination)
                failures.append(f"{row['event_id']} -> {destination}: {error}")
                continue
            con = store.connect()
            try:
                con.execute(
                    """UPDATE outbox_deliveries
                       SET attempts=attempts+1, delivered_at=?, last_error=NULL
                       WHERE event_id=? AND destination=?""",
                    (
                        dt.datetime.now(dt.timezone.utc).isoformat(),
                        row["event_id"],
                        destination,
                    ),
                )
                con.commit()
            finally:
                con.close()
            delivered += 1
    if failures:
        raise RuntimeError("Some outbox events were not delivered: " + "; ".join(failures))
    return delivered


async def run_worker(poll_interval: float = 1.0) -> None:
    while True:
        try:
            await publish_pending()
        except RuntimeError:
            logger.exception("Outbox cycle incomplete; failed deliveries will be retried")
        await asyncio.sleep(poll_interval)


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
