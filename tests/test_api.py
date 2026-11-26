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
                "search_mode": "all",
                "search_field": "all",
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
    assert payload["locations"][0] == {
        "city": "Whitehorse",
        "province": "YT",
        "name": "Whitehorse",
        "value": 400.0,
        "count": 1,
    }
    assert payload["dataset"] == {
        "version": "1.0.0",
        "retrievedAt": "2026-01-01T00:00:00Z",
        "recordCount": 3,
        "isSample": False,
        "latestAmendmentsOnly": False,
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


def test_grants_filters_dates_and_value_range_and_sorts_newest_first(
    tmp_path: Path,
) -> None:
    with TestClient(create_app(_write_fixture(tmp_path))) as client:
        response = client.get(
            "/v1/grants",
            params={
                "start_date": "2025-01-15",
                "end_date": "2025-12-31",
                "minimum_value": 100,
                "maximum_value": 500,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["totalRecords"] == 1
    assert [record["record_id"] for record in payload["records"]] == [
        "grant-2"
    ]


def test_grants_supports_scoped_keyword_modes_and_active_dates(
    tmp_path: Path,
) -> None:
    with TestClient(create_app(_write_fixture(tmp_path))) as client:
        response = client.get(
            "/v1/grants",
            params={
                "query": "Northern missing",
                "search_mode": "any",
                "search_field": "description",
                "date_field": "active",
                "start_date": "2025-01-15",
                "end_date": "2025-02-15",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert [record["record_id"] for record in payload["records"]] == [
        "grant-2"
    ]


def test_grants_supports_value_sort_and_validates_sort(tmp_path: Path) -> None:
    with TestClient(create_app(_write_fixture(tmp_path))) as client:
        response = client.get("/v1/grants", params={"sort": "value_desc"})
        invalid = client.get("/v1/grants", params={"sort": "random"})

    assert response.status_code == 200
    assert [record["record_id"] for record in response.json()["records"]] == [
        "grant-2",
        "grant-1",
        "grant-3",
    ]
    assert invalid.status_code == 422



def _amended_record(
    record_id: str,
    agreement_id: str,
    amendment_number: int,
    value: float,
    *,
    is_latest: bool,
    amendment_count: int,
    province: str = "ON",
    department: str = "Department A",
) -> dict[str, object]:
    return {
        "record_id": record_id,
        "agreement_id": agreement_id,
        "reference_number": "REF-1",
        "amendment_number": amendment_number,
        "recipient_name": f"Recipient {agreement_id}",
        "agreement_title": "Shared project",
        "agreement_value": value,
        "agreement_start_date": date(2025, 1, 1),
        "agreement_end_date": None,
        "province": province,
        "city": "Ottawa",
        "department": department,
        "program_name": "Fund",
        "description": "Purpose",
        "source_url": "https://example.ca/grants.csv",
        "retrieved_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "is_latest_amendment": is_latest,
        "amendment_count": amendment_count,
    }


def _write_amended_fixture(tmp_path: Path) -> Settings:
    records = [
        _amended_record("r-a0", "agreement-a", 0, 100.0, is_latest=False, amendment_count=3),
        _amended_record("r-a1", "agreement-a", 1, 150.0, is_latest=False, amendment_count=3),
        _amended_record("r-a2", "agreement-a", 2, 120.0, is_latest=True, amendment_count=3),
        _amended_record(
            "r-b0",
            "agreement-b",
            0,
            500.0,
            is_latest=True,
            amendment_count=1,
            province="QC",
            department="Department B",
        ),
    ]
    parquet_path = tmp_path / "grants.parquet"
    table = pa.Table.from_pylist(records).cast(
        pa.Table.from_pylist(records).schema.set(
            3, pa.field("amendment_number", pa.int32())
        )
    )
    pq.write_table(table, parquet_path)
    manifest_path = tmp_path / "grants.manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": "2.0.0",
                "source": {"retrieved_at": "2026-01-01T00:00:00Z"},
                "counts": {"accepted": 4, "agreements": 2},
            }
        ),
        encoding="utf-8",
    )
    return Settings(parquet_path=parquet_path, manifest_path=manifest_path)


def test_totals_count_each_agreement_once_at_latest_amendment(tmp_path: Path) -> None:
    with TestClient(create_app(_write_amended_fixture(tmp_path))) as client:
        payload = client.get("/v1/grants", params={"sort": "value_desc"}).json()
        health = client.get("/health").json()

    assert payload["totalRecords"] == 2
    assert payload["totalFunding"] == 620.0
    assert [record["record_id"] for record in payload["records"]] == ["r-b0", "r-a2"]
    assert payload["records"][1]["amendment_number"] == 2
    assert payload["records"][1]["amendment_count"] == 3
    assert payload["records"][1]["agreement_id"] == "agreement-a"
    regions = {region["code"]: region for region in payload["regions"]}
    assert regions["ON"]["value"] == 120.0
    assert regions["ON"]["count"] == 1
    assert {item["name"]: item["value"] for item in payload["departments"]} == {
        "Department B": 500.0,
        "Department A": 120.0,
    }
    assert payload["dataset"]["recordCount"] == 2
    assert payload["dataset"]["latestAmendmentsOnly"] is True
    assert health["recordCount"] == 2


def test_value_filters_use_latest_amended_value(tmp_path: Path) -> None:
    with TestClient(create_app(_write_amended_fixture(tmp_path))) as client:
        payload = client.get("/v1/grants", params={"minimum_value": 140}).json()

    assert [record["record_id"] for record in payload["records"]] == ["r-b0"]


def test_agreement_amendments_returns_full_history(tmp_path: Path) -> None:
    with TestClient(create_app(_write_amended_fixture(tmp_path))) as client:
        response = client.get("/v1/agreements/agreement-a/amendments")
        missing = client.get("/v1/agreements/nope/amendments")

    assert response.status_code == 200
    amendments = response.json()["amendments"]
    assert [item["amendment_number"] for item in amendments] == [0, 1, 2]
    assert [item["agreement_value"] for item in amendments] == [100.0, 150.0, 120.0]
    assert [item["is_latest_amendment"] for item in amendments] == [False, False, True]
    assert missing.status_code == 404


def test_agreement_amendments_unavailable_for_legacy_dataset(tmp_path: Path) -> None:
    with TestClient(create_app(_write_fixture(tmp_path))) as client:
        response = client.get("/v1/agreements/grant-1/amendments")

    assert response.status_code == 404
