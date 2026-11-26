# Maple Data API

Parquet-backed FastAPI service for Maple Atlas.

## Development

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest
```

The service reads:

- `MAPLE_PARQUET_PATH` (default `/srv/maple-atlas/data/grants.parquet`)
- `MAPLE_MANIFEST_PATH` (default `/srv/maple-atlas/data/grants.manifest.json`)

Run it with:

```bash
uvicorn maple_data_api.app:app --host 127.0.0.1 --port 8000
```

Endpoints:

- `GET /health`
- `GET /v1/grants`
- `GET /v1/agreements/{agreement_id}/amendments`
- `GET /v1/recipients/top`
- `GET /v1/recipients/{recipient_id}`

With a Forge schema 2.0.0 dataset, `/v1/grants` returns and totals only the
latest amendment of each agreement, because every amendment row restates the
agreement's full value. Older datasets without amendment flags are served as
before, and `dataset.latestAmendmentsOnly` reports which mode is active. The
amendments endpoint returns every amendment of one agreement, oldest first.

With a Forge schema 2.1.0 dataset, recipient profiles group every agreement
by Forge's `recipient_id`: total funding, funding by agreement start year,
departments, programs, largest agreements and, for individuals, the
institution whose business number their grants are filed under. A person's
own business number is never returned. `/v1/recipients/top` lists the
largest recipients (organizations only unless `include_people=true`) and
always excludes placeholder entries such as "Batch report" and "Withheld".
Maple Beacon uses both endpoints.

Set `MAPLE_CHARITIES_PATH` to Forge's `charities.parquet` to add a `charity`
section (CRA T3010 return: revenue by level of government, spending, assets)
to profiles of recipients that are registered charities. Without it, `charity`
is always `null`.

Grant queries support scoped keyword search, province, department,
start/end/active date ranges, funding ranges, pagination, location summaries,
and sort parameters. Records default to newest agreement start date first, with
missing dates last.

See [docs/HANDOFF.md](docs/HANDOFF.md) for the production architecture,
server paths, deployment procedure, dataset refresh process, and
troubleshooting guide.
