from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from maple_data_api.app import Settings, create_app


def _write_fixture(tmp_path: Path) -> Settings:
    records = [
        {
            "record_id": "grant-1",
            "recipient_name": "Community Group",
            "agreement_title": "Youth program",
            "agreement_value": 100.0,
            "agreement_start_date": date(2025, 1, 1),
            "agreement_end_date": None,
            "province": "ON",
            "city": "Ottawa",
            "department": "Department A",
            "program_name": "Community Fund",
            "description": "Local programming",
            "source_url": "https://example.ca/1",
            "retrieved_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
        {
            "record_id": "grant-2",
            "recipient_name": "Northern Lab",
            "agreement_title": "Climate research",
            "agreement_value": 400.0,
            "agreement_start_date": date(2025, 2, 1),
            "agreement_end_date": None,
            "province": "YT",
            "city": "Whitehorse",
            "department": "Department B",
            "program_name": "Research Fund",
            "description": "Northern research",
            "source_url": "https://example.ca/2",
            "retrieved_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
        {
            "record_id": "grant-3",
            "recipient_name": "Unknown Group",
            "agreement_title": "Climate support",
            "agreement_value": 50.0,
            "agreement_start_date": None,
            "agreement_end_date": None,
            "province": None,
            "city": None,
            "department": "Department B",
            "program_name": None,
            "description": "",
            "source_url": "https://example.ca/3",
            "retrieved_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
    ]
    parquet_path = tmp_path / "grants.parquet"
    pq.write_table(pa.Table.from_pylist(records), parquet_path)

    manifest_path = tmp_path / "grants.manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "source": {"retrieved_at": "2026-01-01T00:00:00Z"},
                "counts": {"accepted": 3},
            }
        ),
        encoding="utf-8",
    )
    return Settings(parquet_path=parquet_path, manifest_path=manifest_path)


def test_health_reports_dataset_count(tmp_path: Path) -> None:
    with TestClient(create_app(_write_fixture(tmp_path))) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "recordCount": 3}


def test_grants_matches_atlas_contract(tmp_path: Path) -> None:
    with TestClient(create_app(_write_fixture(tmp_path))) as client:
        response = client.get(
            "/v1/grants",
            params={
                "query": "climate",
                "province": "YT",
                "department": "Department B",
                "page": 1,
                "page_size": 25,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["totalRecords"] == 1
    assert payload["totalFunding"] == 400.0
    assert payload["recipientCount"] == 1
    assert payload["records"][0]["record_id"] == "grant-2"
    assert payload["regions"][0] == {
        "code": "YT",
        "name": "Yukon",
        "value": 400.0,
        "count": 1,
    }
    assert payload["unknownRegion"] == {
        "name": "Unknown location",
        "value": 50.0,
        "count": 1,
    }
    assert payload["dataset"] == {
        "version": "1.0.0",
        "retrievedAt": "2026-01-01T00:00:00Z",
        "recordCount": 3,
        "isSample": False,
    }
    assert payload["provinceOptions"] == ["ON", "YT"]
    assert payload["departmentOptions"] == ["Department A", "Department B"]


def test_grants_clamps_page_and_rejects_large_pages(tmp_path: Path) -> None:
    with TestClient(create_app(_write_fixture(tmp_path))) as client:
        response = client.get(
            "/v1/grants", params={"page": 99, "page_size": 2}
        )
        invalid = client.get("/v1/grants", params={"page_size": 101})

    assert response.status_code == 200
    assert response.json()["page"] == 2
    assert response.json()["totalPages"] == 2
    assert invalid.status_code == 422

