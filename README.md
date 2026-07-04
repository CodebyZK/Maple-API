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

