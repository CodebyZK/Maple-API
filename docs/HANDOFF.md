# Maple Data API: Architecture and Operations Handoff

Last verified: 2026-07-04

## Purpose

Maple Data API gives Maple Atlas searchable access to the complete federal
grants dataset. The browser does not download the 98 MB Parquet file. Instead,
Atlas requests one bounded page plus aggregate totals from this API.

The production deployment currently serves 1,264,036 records.

## Repositories

| Component | Repository | Purpose |
| --- | --- | --- |
| Atlas | `https://git.zionkoudijs.com/studio-zk/maple/atlas.git` | React/Vite frontend |
| Data API | `https://git.zionkoudijs.com/studio-zk/maple/data-api.git` | FastAPI and DuckDB service |
| Forge | Maple Forge repository in the same Maple project | Downloads, cleans, validates, and exports the dataset |

The Data API repository contains:

```text
data-api/
├── docs/HANDOFF.md
├── src/maple_data_api/app.py
├── tests/test_api.py
├── pyproject.toml
└── README.md
```

`src/maple_data_api/app.py` is the production application. It defines the
FastAPI service, validates dataset paths during startup, queries Parquet with
DuckDB, and returns the Atlas response contract.

## Production request flow

```text
Browser
  |
  | HTTPS https://atlas.zionkoudijs.com
  v
Cloudflare
  |
  | Cloudflare Tunnel: homelab
  v
cloudflared on zkserver
  |
  | http://localhost:80
  v
Nginx
  |-- /             -> /var/www/atlas
  |
  `-- /api/*        -> http://127.0.0.1:8000/*
                           |
                           v
                     Maple Data API
                           |
                           v
                  DuckDB reads grants.parquet
```

Cloudflare terminates public HTTPS. The tunnel connects to Nginx over the
server's loopback interface. The API listens only on `127.0.0.1:8000`; it is
not directly exposed to the internet.

Atlas and the API share one public origin. This avoids cross-origin requests
and means no CORS configuration is required.

## Production server paths

All paths below are on `zkserver`.

| Path | Contents |
| --- | --- |
| `/var/www/atlas` | Deployed Atlas static build |
| `/home/zkoudijs/apps/atlas` | Atlas Git working tree |
| `/srv/maple-atlas/data/grants.parquet` | Full production dataset |
| `/srv/maple-atlas/data/grants.manifest.json` | Forge lineage, counts, and checksums |
| `/srv/maple-atlas/api/source` | Data API Git working tree |
| `/srv/maple-atlas/api/.venv` | Data API Python virtual environment |
| `/etc/systemd/system/maple-data-api.service` | API systemd unit |
| `/etc/nginx/sites-available/atlas` | Atlas Nginx virtual host |
| `/etc/nginx/sites-enabled/atlas` | Symlink enabling the virtual host |
| `/home/zkoudijs/.cloudflared/config.yml` | Cloudflare Tunnel ingress rules |

The Parquet file is intentionally outside `/var/www`. Nginx cannot serve the
dataset directly.

## DNS and Cloudflare Tunnel

`atlas.zionkoudijs.com` routes to the Cloudflare Tunnel with ID:

```text
3c6c0fb1-d45f-4b6c-af7d-afcbcd6535bf
```

This ID is an identifier, not a credential.

The relevant tunnel ingress rule is:

```yaml
- hostname: atlas.zionkoudijs.com
  service: http://localhost:80
```

It must appear before the final catch-all rule:

```yaml
- service: http_status:404
```

Useful checks:

```bash
cloudflared tunnel ingress validate
systemctl is-active cloudflared
sudo journalctl -u cloudflared -n 50 --no-pager
```

## Nginx configuration

The production virtual host is:

```nginx
server {
    listen 80;
    server_name atlas.zionkoudijs.com;

    root /var/www/atlas;
    index index.html;

    location /api/ {
        proxy_pass http://127.0.0.1:8000/;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_connect_timeout 5s;
        proxy_read_timeout 30s;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

The trailing slash on `proxy_pass` is significant. It maps:

```text
/api/v1/grants -> http://127.0.0.1:8000/v1/grants
/api/health    -> http://127.0.0.1:8000/health
```

Validate and reload Nginx after changes:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## API process

systemd runs one Uvicorn worker:

```text
/srv/maple-atlas/api/.venv/bin/uvicorn \
  maple_data_api.app:app \
  --host 127.0.0.1 \
  --port 8000 \
  --workers 1
```

One worker avoids unnecessary duplicated memory and is adequate for the
current traffic level. DuckDB opens an in-memory connection per request and
queries the read-only Parquet dataset.

The systemd service sets:

```text
MAPLE_PARQUET_PATH=/srv/maple-atlas/data/grants.parquet
MAPLE_MANIFEST_PATH=/srv/maple-atlas/data/grants.manifest.json
```

Service operations:

```bash
systemctl status maple-data-api --no-pager
sudo systemctl restart maple-data-api
sudo journalctl -u maple-data-api -n 50 --no-pager
curl http://127.0.0.1:8000/health
```

The service is enabled at boot and restarts after failures.

## API endpoints

### `GET /health`

Example response:

```json
{
  "status": "ok",
  "recordCount": 1264036
}
```

### `GET /v1/grants`

Query parameters:

| Parameter | Default | Rules |
| --- | --- | --- |
| `query` | empty | Maximum 200 characters |
| `province` | empty | Exact province/territory code |
| `department` | empty | Exact department name |
| `start_date` | empty | Agreement start date on or after this ISO date |
| `end_date` | empty | Agreement start date on or before this ISO date |
| `minimum_value` | empty | Non-negative minimum agreement value |
| `sort` | `date_desc` | `date_desc`, `date_asc`, `value_desc`, or `value_asc` |
| `page` | `1` | Minimum 1 |
| `page_size` | `25` | From 1 through 100 |

Example:

```bash
curl "http://127.0.0.1:8000/v1/grants?query=climate&page=1&page_size=25"
```

The response includes:

- the requested record page;
- page and total-record metadata;
- matching funding and recipient totals;
- the six largest matching departments;
- totals for all 13 regions;
- unknown-region totals;
- province and department filter options; and
- Forge dataset lineage.

Region totals apply the text and department filters but deliberately ignore
the selected province. Atlas needs this behavior so users can move directly
between map regions.

Record pages default to newest agreement start date first, with undated
agreements last. Every sort uses `record_id` as a stable tie-breaker.

## Dataset lineage and integrity

The currently deployed manifest reports:

```text
schema version: 1.1.0
retrieved at:   2026-07-04T17:21:35.539541Z
record count:   1,264,036
```

The deployed Parquet SHA-256 verified on 2026-07-04 was:

```text
b0c6b3f6783ee4b026d8e8b3fd1af52c8a24d5b567472ca74e59ee90ce2b99e7
```

Verify the deployed artifact against its manifest:

```bash
sha256sum /srv/maple-atlas/data/grants.parquet
grep -A4 '"parquet"' /srv/maple-atlas/data/grants.manifest.json
```

The values must match before restarting the API.

## Atlas configuration and deployment

Atlas must be built with:

```text
VITE_DATA_API_URL=/api/v1
```

Vite embeds this value at build time. Changing the environment after a build
does not change an already deployed frontend.

Current manual deployment:

```bash
cd /home/zkoudijs/apps/atlas
git pull --ff-only
npm ci
npm test
env VITE_DATA_API_URL=/api/v1 npm run build
cp -a dist/. /var/www/atlas/
```

Without `VITE_DATA_API_URL`, Atlas silently uses its committed 500-record
development sample. Always confirm the production dataset label after a
deployment.

## Data API deployment

Current manual deployment:

```bash
cd /srv/maple-atlas/api/source
git pull --ff-only
/srv/maple-atlas/api/.venv/bin/pip install -e .
sudo systemctl restart maple-data-api
curl http://127.0.0.1:8000/health
```

For a dependency update, run the repository tests before restarting:

```bash
cd /srv/maple-atlas/api/source
/srv/maple-atlas/api/.venv/bin/pip install -e ".[dev]"
/srv/maple-atlas/api/.venv/bin/pytest
```

## Forge dataset refresh

Forge remains the source of truth. Do not edit Parquet records manually.

Build a refreshed dataset in Forge:

```bash
maple-forge-download
maple-forge-build
```

This produces:

```text
forge/data/processed/grants.parquet
forge/data/processed/grants.manifest.json
```

Upload both files to temporary server names first:

```powershell
scp `
  "forge\data\processed\grants.parquet" `
  "forge\data\processed\grants.manifest.json" `
  zkoudijs@zkserver:/srv/maple-atlas/data/incoming/
```

Before using this procedure for the first time, create the staging directory:

```bash
install -d -m 0750 /srv/maple-atlas/data/incoming
```

On the server:

1. Verify the incoming Parquet checksum against the incoming manifest.
2. Confirm the incoming manifest has a plausible nonzero accepted count.
3. Replace both production files together during a short maintenance window.
4. Restart `maple-data-api`.
5. Check `/health` and one representative `/v1/grants` query.
6. Confirm the Atlas dataset label reports the new lineage and count.

Do not overwrite the live Parquet file while the API is querying it. Use
same-filesystem renames so publication is atomic. Retain the previous pair
until the new deployment passes its smoke tests.

The exact atomic refresh should be scripted before routine scheduled updates;
it is not automated yet.

## Verification commands

Internal API:

```bash
curl http://127.0.0.1:8000/health
```

Public API:

```bash
curl https://atlas.zionkoudijs.com/api/health
```

Representative full-dataset query:

```bash
curl -s \
  "https://atlas.zionkoudijs.com/api/v1/grants?query=climate&page=1&page_size=2" \
  -o /tmp/maple-query.json

python3 -c \
  "import json; d=json.load(open('/tmp/maple-query.json')); print(d['totalRecords'], len(d['records']), d['dataset'])"
```

At initial deployment, the query `climate` returned 9,481 matches. That number
may change after a dataset refresh.

## Tests

The API contract tests use a synthetic Parquet fixture:

```bash
pip install -e ".[dev]"
pytest
```

They validate:

- health and dataset count;
- search, province, and department filtering;
- response fields expected by Atlas;
- region aggregation behavior;
- lineage metadata;
- page clamping; and
- maximum page-size enforcement.

The initial release also passed a real query against all 1,264,036 records.

## Security boundaries

- The API binds only to `127.0.0.1`.
- The Parquet file is not under the Nginx document root.
- Public traffic enters through the Cloudflare Tunnel.
- Atlas and the API are same-origin, so permissive CORS is unnecessary.
- Query values are passed to DuckDB as parameters.
- Page size is capped at 100.
- systemd applies `NoNewPrivileges`, `PrivateTmp`, `ProtectHome`, and
  filesystem protection.
- Cloudflare credentials must never be committed or copied into this
  repository.

## Known operational gaps

The production service works, but these items remain:

1. Add GitLab CI to run Data API tests on every merge request.
2. Automate Data API deployment instead of pulling manually.
3. Replace Atlas's GitLab Pages deployment with an Nginx deployment job.
4. Set `VITE_DATA_API_URL=/api/v1` in the Atlas CI build environment.
5. Script and test atomic Forge dataset publication and rollback.
6. Add monitoring for health, latency, process restarts, and disk space.
7. Consider caching common aggregate queries if production traffic makes the
   current roughly 1.5-second uncached text-search latency insufficient.

Do not describe the deployment as fully automated until these gaps are closed.

## Troubleshooting

### Atlas shows only 500 records

The frontend was built without `VITE_DATA_API_URL=/api/v1`. Rebuild and
redeploy Atlas with that value.

### `/api/health` returns the Atlas HTML page

The Nginx `/api/` location is missing, not loaded, or the request reached a
different virtual host. Run `sudo nginx -T`, validate the Atlas server block,
then reload Nginx.

### API returns 502 through Nginx

Check:

```bash
systemctl status maple-data-api --no-pager
sudo journalctl -u maple-data-api -n 50 --no-pager
curl http://127.0.0.1:8000/health
```

### Public hostname does not resolve

Check the Cloudflare Tunnel DNS route and allow time for negative DNS caches
to expire:

```bash
dig +short atlas.zionkoudijs.com A @1.1.1.1
cloudflared tunnel route dns \
  3c6c0fb1-d45f-4b6c-af7d-afcbcd6535bf \
  atlas.zionkoudijs.com
```

### Service fails during startup

Confirm both configured files exist and are readable by `zkoudijs`:

```bash
ls -lh /srv/maple-atlas/data
sudo journalctl -u maple-data-api -n 100 --no-pager
```
