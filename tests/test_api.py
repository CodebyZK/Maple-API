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


def _profile_record(
    record_id: str,
    recipient_id: str,
    name: str,
    value: float,
    *,
    start: date | None,
    department: str = "Department A",
    is_person: bool = False,
    business_number: str | None = None,
    recipient_type: str = "F",
    is_latest: bool = True,
) -> dict[str, object]:
    return {
        "record_id": record_id,
        "agreement_id": f"agreement-{record_id}",
        "reference_number": record_id,
        "amendment_number": 0,
        "recipient_name": name,
        "recipient_type": recipient_type,
        "business_number": business_number,
        "agreement_title": f"Project {record_id}",
        "agreement_value": value,
        "agreement_start_date": start,
        "agreement_end_date": None,
        "province": "ON",
        "city": "Toronto",
        "department": department,
        "program_name": "Fund",
        "description": "Purpose",
        "source_url": "https://example.ca/grants.csv",
        "retrieved_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "is_latest_amendment": is_latest,
        "amendment_count": 1,
        "recipient_id": recipient_id,
        "recipient_is_person": is_person,
        "recipient_is_aggregate": recipient_id == "rec-batch",
    }


def _write_profile_fixture(tmp_path: Path) -> Settings:
    records = [
        _profile_record("a1", "rec-acme", "Acme Inc.", 100.0, start=date(2023, 4, 1), business_number="111111112"),
        _profile_record("a2", "rec-acme", "ACME INCORPORATED", 300.0, start=date(2024, 5, 1), department="Department B", business_number="111111112"),
        _profile_record("a3", "rec-acme", "Acme Inc.", 50.0, start=None, business_number="111111112"),
        _profile_record("a4", "rec-acme", "Acme Inc.", 999.0, start=date(2024, 6, 1), business_number="111111112", is_latest=False),
        _profile_record("u1", "rec-uni", "University of Example", 1000.0, start=date(2022, 1, 1), recipient_type="S", business_number="222222223"),
        _profile_record("p1", "rec-person", "Doe, Jane", 20.0, start=date(2024, 1, 1), recipient_type="P", is_person=True, business_number="222222223"),
        _profile_record("b1", "rec-batch", "Batch report | Rapport en lots", 5000.0, start=date(2024, 1, 1), recipient_type="P"),
    ]
    parquet_path = tmp_path / "grants.parquet"
    table = pa.Table.from_pylist(records)
    table = table.cast(table.schema.set(3, pa.field("amendment_number", pa.int32())))
    pq.write_table(table, parquet_path)
    manifest_path = tmp_path / "grants.manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": "2.1.0",
                "source": {"retrieved_at": "2026-01-01T00:00:00Z"},
                "counts": {"accepted": 6, "agreements": 5},
            }
        ),
        encoding="utf-8",
    )
    return Settings(parquet_path=parquet_path, manifest_path=manifest_path)


def test_recipient_profile_totals_latest_amendments_by_year(tmp_path: Path) -> None:
    with TestClient(create_app(_write_profile_fixture(tmp_path))) as client:
        response = client.get("/v1/recipients/rec-acme")
        grants = client.get("/v1/grants", params={"query": "acme"}).json()

    assert response.status_code == 200
    profile = response.json()
    assert profile["name"] == "Acme Inc."
    assert profile["otherNames"] == ["ACME INCORPORATED"]
    assert profile["isPerson"] is False
    assert profile["isAggregate"] is False
    assert profile["recipientTypeLabel"] == "For-profit organization"
    assert profile["businessNumber"] == "111111112"
    assert profile["totalFunding"] == 450.0
    assert profile["agreementCount"] == 3
    assert profile["byYear"] == [
        {"year": 2023, "value": 100.0, "count": 1},
        {"year": 2024, "value": 300.0, "count": 1},
    ]
    assert profile["undated"] == {"value": 50.0, "count": 1}
    assert profile["departments"][0]["name"] == "Department B"
    assert [item["record_id"] for item in profile["largestAgreements"]] == ["a2", "a1", "a3"]
    assert profile["affiliation"] is None
    assert grants["records"][0]["recipient_id"] == "rec-acme"


def test_person_profile_hides_business_number_and_shows_affiliation(tmp_path: Path) -> None:
    with TestClient(create_app(_write_profile_fixture(tmp_path))) as client:
        profile = client.get("/v1/recipients/rec-person").json()

    assert profile["isPerson"] is True
    assert profile["businessNumber"] is None
    assert profile["affiliation"] == {
        "name": "University of Example",
        "businessNumber": "222222223",
        "agreementCount": 1,
    }


def test_top_recipients_excludes_people_by_default(tmp_path: Path) -> None:
    with TestClient(create_app(_write_profile_fixture(tmp_path))) as client:
        organizations = client.get("/v1/recipients/top", params={"limit": 10}).json()
        everyone = client.get(
            "/v1/recipients/top", params={"limit": 10, "include_people": True}
        ).json()
        missing = client.get("/v1/recipients/nope")
        batch = client.get("/v1/recipients/rec-batch").json()

    assert [item["recipientId"] for item in organizations["recipients"]] == [
        "rec-uni",
        "rec-acme",
    ]
    assert len(everyone["recipients"]) == 3
    assert missing.status_code == 404
    assert batch["isAggregate"] is True


def test_recipient_endpoints_unavailable_without_recipient_columns(tmp_path: Path) -> None:
    with TestClient(create_app(_write_amended_fixture(tmp_path))) as client:
        assert client.get("/v1/recipients/top").status_code == 404
        assert client.get("/v1/recipients/x").status_code == 404



def _write_charities(tmp_path: Path) -> Path:
    path = tmp_path / "charities.parquet"
    rows = [{
        "business_number": "111111112",
        "registration_number": "111111112RR0001",
        "recipient_id": "rec-acme",
        "legal_name": "ACME FOUNDATION",
        "designation": "Charitable organization",
        "category": "Research",
        "city": "TORONTO",
        "province": "ON",
        "fiscal_period_end": date(2024, 3, 31),
        "section_used": "6",
        "revenue_federal": 380.0,
        "revenue_provincial": 1200.0,
        "revenue_municipal": None,
        "revenue_government_reported": None,
        "total_revenue": 3700.0,
        "charitable_programs": 3300.0,
        "management_and_administration": 200.0,
        "fundraising": None,
        "total_expenditures": 3500.0,
        "total_assets": 8300.0,
        "total_liabilities": 5600.0,
        "revenue_government_total": 1580.0,
        "list_year": "2024",
    }]
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def test_recipient_profile_includes_linked_charity(tmp_path: Path) -> None:
    settings = _write_profile_fixture(tmp_path)
    settings = Settings(
        parquet_path=settings.parquet_path,
        manifest_path=settings.manifest_path,
        charities_path=_write_charities(tmp_path),
    )
    with TestClient(create_app(settings)) as client:
        acme = client.get("/v1/recipients/rec-acme").json()
        other = client.get("/v1/recipients/rec-uni").json()

    charity = acme["charity"]
    assert charity["registrationNumber"] == "111111112RR0001"
    assert charity["fiscalPeriodEnd"] == "2024-03-31"
    assert charity["amounts"]["revenue_federal"] == 380.0
    assert charity["amounts"]["revenue_municipal"] is None
    assert charity["amounts"]["revenue_government_total"] == 1580.0
    assert "open.canada.ca" in charity["sourceUrl"]
    assert other["charity"] is None


def test_profiles_have_no_charity_without_charity_data(tmp_path: Path) -> None:
    with TestClient(create_app(_write_profile_fixture(tmp_path))) as client:
        profile = client.get("/v1/recipients/rec-acme").json()

    assert profile["charity"] is None



def _write_network_fixture(tmp_path: Path) -> Settings:
    def row(record_id, recipient_id, name, value, program, *, person=False, kind="F"):
        return {
            **_profile_record(record_id, recipient_id, name, value, start=date(2024, 1, 1), recipient_type=kind, is_person=person),
            "program_name": program,
        }

    records = [
        row("a1", "rec-acme", "Acme Inc.", 20_000.0, "Clean Tech"),
        row("a2", "rec-acme", "Acme Inc.", 2_000_000.0, "Clean Tech"),
        row("a3", "rec-acme", "Acme Inc.", 500_000.0, "Exports"),
        row("b1", "rec-beta", "Beta Corp", 3_000_000.0, "Clean Tech"),
        row("b2", "rec-beta", "Beta Corp", 100_000.0, "Exports"),
        row("c1", "rec-gamma", "Gamma Ltd", 50_000.0, "Clean Tech"),
        row("d1", "rec-delta", "Delta Farms", 900_000.0, "Farming"),
        row("b3", "rec-beta-2", "BETA CORP", 10_000.0, "Clean Tech"),
        row("p1", "rec-person", "Doe, Jane", 10_000.0, "Clean Tech", person=True, kind="P"),
    ]
    parquet_path = tmp_path / "grants.parquet"
    table = pa.Table.from_pylist(records)
    table = table.cast(table.schema.set(3, pa.field("amendment_number", pa.int32())))
    pq.write_table(table, parquet_path)
    manifest_path = tmp_path / "grants.manifest.json"
    manifest_path.write_text(
        json.dumps({"schema_version": "2.1.0", "source": {"retrieved_at": "2026-01-01T00:00:00Z"}, "counts": {"accepted": 9, "agreements": 9}}),
        encoding="utf-8",
    )
    return Settings(parquet_path=parquet_path, manifest_path=manifest_path)


def test_similar_recipients_share_programs_and_exclude_people(tmp_path: Path) -> None:
    with TestClient(create_app(_write_network_fixture(tmp_path))) as client:
        acme = client.get("/v1/recipients/rec-acme/similar").json()["similar"]
        person = client.get("/v1/recipients/rec-person/similar").json()["similar"]

    # "BETA CORP" is a second profile with the same name: listed once.
    assert [item["recipientId"] for item in acme] == ["rec-beta", "rec-gamma"]
    assert acme[0]["sharedPrograms"] == ["Clean Tech", "Exports"]
    assert acme[0]["totalFunding"] == 3_100_000.0
    assert "rec-person" not in [item["recipientId"] for item in acme]
    assert "rec-delta" not in [item["recipientId"] for item in acme]
    assert person == []


def test_profile_has_size_buckets_and_program_rank(tmp_path: Path) -> None:
    with TestClient(create_app(_write_network_fixture(tmp_path))) as client:
        profile = client.get("/v1/recipients/rec-acme").json()

    buckets = {item["label"]: item for item in profile["sizeBuckets"]}
    assert buckets["Under $25K"]["count"] == 1
    assert buckets["$100K to $1M"]["count"] == 1
    assert buckets["$1M to $10M"] == {"label": "$1M to $10M", "count": 1, "value": 2_000_000.0}
    assert buckets["$10M and over"]["count"] == 0
    rank = profile["programRank"]
    assert rank["program"] == "Clean Tech"
    assert rank["rank"] == 2
    assert rank["recipients"] == 5
    assert round(rank["share"], 3) == round(2_020_000 / 5_090_000, 3)
