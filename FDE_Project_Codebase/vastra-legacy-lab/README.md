# Vastra legacy lab (S1-S2)

A runnable stand-in for Vastra & Co.'s legacy estate and its incremental target architecture, plus three months
of synthetic logs for evidence collection. The 2014 monolith remains available as a separate legacy service.

## What is inside

| Path | What it is |
|---|---|
| `app/monolith.py` | The 2014 ecommerce monolith: server-rendered product page, private app endpoint, sequential checkout |
| `app/pricing_web.py`, `app/app_api.py`, `pos/pricing_pos.py` | Three copies of the pricing and promotion rules that disagree (R4) |
| `app/partners/stubs.py` | Partner APIs (courier, payment gateway, SMS and email): the only real APIs today |
| `app/jobs/` | Batch jobs: stock sync every 4 hours (R5), nightly orders to ERP (E14), nightly price list |
| `app/v1/api.py` | New FastAPI service on port 8200; legacy catalogue reads are read-only |
| `app/v1/store.py` | Separate v1 database for live stock, reservations, audit, idempotency, and outbox |
| `app/v1/pricing.py` | Shared pricing policy `vastra-2026-11-v1` |
| `app/v1/outbox.py` | Retryable event publisher for the warehouse and ERP |
| `pos/v1_client.py` | POS adapter; calls API v1 through the NGINX gateway |
| `gateway/nginx.conf` | NGINX gateway on port 8080, request IDs, access log, and 5-second proxy timeouts |
| `config.ini` | One shared database login in plain text (R15a) |
| `tests/test_legacy_characterisation.py` | Tests that pin down today's defects (they pass now; S2 replaces them) |
| `tools/generate_logs.py` | Regenerates the logs (12 Aug - 10 Nov 2026) |
| `data/logs/` | 26 log files for evidence E1-E29 |
| `evidence/` | Evidence map, loader, SQL query pack and the facilitator answer key |
| `docs/Vastra_Change_Summary.docx` | Implementation summary, changed files, verification, and follow-up work |
| `docs/Vastra_API_v1_LLD.docx` | As-built low-level design for the v1 service and its architecture boundaries |
| `docs/Vastra_API_v1_Architecture.png` | Architecture diagram embedded in the v1 low-level design |
| `docs/Vastra_Test_Cases.docx` | Executed v1/legacy cases, test results, and recommended future integration tests |

## Run it

Run these commands from `FDE_Project_Codebase\vastra-legacy-lab` in separate terminals. Initialize the
legacy database once for a fresh lab; `python -m app.db` recreates its tables and seed data.

```bash
# Create and activate the environment (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1

pip install -r requirements.txt
python -m app.db                                      # one-time legacy DB initialization
```

Start each service in its own terminal:

```bash
python -m uvicorn app.partners.stubs:app --port 8100       # partner-system stubs
python -m uvicorn app.monolith:app --port 8000             # unchanged legacy monolith
python -m uvicorn app.v1.api:app --host 127.0.0.1 --port 8200
python -m app.v1.outbox                                  # publish committed events; retries every second
```

The existing gateway configuration is `..\gateway\nginx.conf`. Run NGINX with this config on port 8080.
Its upstreams use `host.docker.internal` for containerized NGINX; use `127.0.0.1` for native NGINX.
For containerized NGINX, bind the backend services to `0.0.0.0`; for native NGINX, keep the loopback bind
and change the upstream hostnames to `127.0.0.1`.
For local gateway-routed requests:

```bash
curl http://localhost:8080/v1/catalog/SKU00003
curl -H 'Idempotency-Key: demo-order-1' -H 'content-type: application/json' -d '{"sku":"SKU00003","coupon_pct":10,"member":true}' http://localhost:8080/v1/checkout
curl http://localhost:8080/legacy/product/SKU00003
```

Without NGINX, the v1 service is available at `http://localhost:8200`; partner and monolith ports remain 8100
and 8000. POS integrations should use `pos.v1_client` and the gateway URL, not connect directly to v1.
The legacy POS and monolith modules remain as historical implementations for characterization.

The v1 service exposes:

- `GET /v1/catalog/{sku}` with `ETag` and conditional `304` responses.
- `GET /v1/prices/{sku}` using the shared v1 pricing policy.
- `POST /v1/pricing/overrides` and `DELETE /v1/pricing/overrides/{override_id}` with actor/reason audit records.
- `GET /v1/stock/{sku}` and `POST /v1/stock/events` for event-driven inventory updates (including POS events).
- `POST /v1/checkout` requiring an `Idempotency-Key`; courier and payment calls run in parallel, with a courier
  fallback after 0.8 seconds and a 3-second payment timeout.

Orders and stock changes are committed to the local outbox before they are sent to warehouse/ERP consumers.
The stock ledger tracks inventory by SKU and location (`online` plus individual POS store IDs). The outbox
worker retries failed deliveries; consumers deduplicate by event ID.

Run the characterization and v1 tests:

```bash
pytest -q
```

The checkout response includes a `trace` with the time of each step (W2.1-W2.5): compare it with `checkout_traces_sample.csv`.

## Collect evidence (S1, Activity 2)

```bash
python evidence/load_logs.py                       # builds evidence/evidence.db
sqlite3 evidence/evidence.db < evidence/queries.sql
```

See `evidence/EVIDENCE_MAP.md` for which file proves which evidence item. Facilitators can print every figure with
`python evidence/compute_evidence.py`.

## Notes

- The lab uses SQLite in place of the monolith's MySQL so it runs anywhere; tables and behaviour are the same.
- Large logs are samples (for example, orders are a 10% sample); the evidence map says which.
