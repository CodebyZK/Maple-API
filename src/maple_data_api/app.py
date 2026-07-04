"""Parquet-backed HTTP API for Maple Atlas."""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

import duckdb
from fastapi import FastAPI, Query, Request

REGIONS = (
    ("YT", "Yukon"),
    ("NT", "Northwest Territories"),
    ("NU", "Nunavut"),
    ("BC", "British Columbia"),
    ("AB", "Alberta"),
    ("SK", "Saskatchewan"),
    ("MB", "Manitoba"),
    ("ON", "Ontario"),
    ("QC", "Quebec"),
    ("NB", "New Brunswick"),
    ("PE", "Prince Edward Island"),
    ("NS", "Nova Scotia"),
    ("NL", "Newfoundland and Labrador"),
)

RECORD_SELECT_COLUMNS = (
    "record_id",
    "recipient_name",
    "agreement_title",
    "agreement_value",
    "agreement_start_date",
    "agreement_end_date",
    "province",
    "city",
    "department",
    "program_name",
    "description",
    "source_url",
    "cast(retrieved_at AS VARCHAR) AS retrieved_at",
)

SEARCH_COLUMNS = (
    "recipient_name",
    "agreement_title",
    "program_name",
    "department",
    "description",
    "city",
    "province",
)

SORT_ORDERS = {
    "date_desc": "agreement_start_date DESC NULLS LAST, record_id ASC",
    "date_asc": "agreement_start_date ASC NULLS LAST, record_id ASC",
    "value_desc": "agreement_value DESC, record_id ASC",
    "value_asc": "agreement_value ASC, record_id ASC",
}


@dataclass(frozen=True)
class Settings:
    parquet_path: Path
    manifest_path: Path

    @classmethod
    def from_environment(cls) -> Settings:
        return cls(
            parquet_path=Path(
                os.environ.get(
                    "MAPLE_PARQUET_PATH",
                    "/srv/maple-atlas/data/grants.parquet",
                )
            ),
            manifest_path=Path(
                os.environ.get(
                    "MAPLE_MANIFEST_PATH",
                    "/srv/maple-atlas/data/grants.manifest.json",
                )
            ),
        )


def _quoted_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _where(
    *,
    query: str,
    province: str,
    department: str,
    start_date: date | None = None,
    end_date: date | None = None,
    minimum_value: float | None = None,
    include_province: bool = True,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    parameters: list[Any] = []

    query = query.strip()
    if query:
        clauses.append(
            "("
            + " OR ".join(
                f"contains(lower(coalesce({column}, '')), lower(?))"
                for column in SEARCH_COLUMNS
            )
            + ")"
        )
        parameters.extend([query] * len(SEARCH_COLUMNS))
    if include_province and province:
        clauses.append("province = ?")
        parameters.append(province)
    if department:
        clauses.append("department = ?")
        parameters.append(department)
    if start_date is not None:
        clauses.append("agreement_start_date >= ?")
        parameters.append(start_date)
    if end_date is not None:
        clauses.append("agreement_start_date <= ?")
        parameters.append(end_date)
    if minimum_value is not None:
        clauses.append("agreement_value >= ?")
        parameters.append(minimum_value)

    return (" WHERE " + " AND ".join(clauses) if clauses else ""), parameters


def _rows(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _dataset_info(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": str(manifest["schema_version"]),
        "retrievedAt": str(manifest["source"]["retrieved_at"]),
        "recordCount": int(manifest["counts"]["accepted"]),
        "isSample": False,
    }


def create_app(settings: Settings | None = None) -> FastAPI:
    configured_settings = settings or Settings.from_environment()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if not configured_settings.parquet_path.is_file():
            raise RuntimeError(
                f"Parquet dataset not found: {configured_settings.parquet_path}"
            )
        if not configured_settings.manifest_path.is_file():
            raise RuntimeError(
                f"Dataset manifest not found: {configured_settings.manifest_path}"
            )

        manifest = json.loads(
            configured_settings.manifest_path.read_text(encoding="utf-8")
        )
        source = (
            f"read_parquet('{_quoted_path(configured_settings.parquet_path)}')"
        )
        with duckdb.connect() as connection:
            province_options = [
                row[0]
                for row in connection.execute(
                    f"SELECT DISTINCT province FROM {source} "
                    "WHERE province IS NOT NULL ORDER BY province"
                ).fetchall()
            ]
            department_options = [
                row[0]
                for row in connection.execute(
                    f"SELECT DISTINCT department FROM {source} "
                    "ORDER BY department"
                ).fetchall()
            ]

        app.state.source = source
        app.state.dataset = _dataset_info(manifest)
        app.state.province_options = province_options
        app.state.department_options = department_options
        yield

    app = FastAPI(
        title="Maple Data API",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        return {
            "status": "ok",
            "recordCount": request.app.state.dataset["recordCount"],
        }

    @app.get("/v1/grants")
    def grants(
        request: Request,
        query: str = Query(default="", max_length=200),
        province: str = Query(default="", max_length=100),
        department: str = Query(default="", max_length=300),
        start_date: date | None = Query(default=None),
        end_date: date | None = Query(default=None),
        minimum_value: float | None = Query(default=None, ge=0),
        sort: Literal[
            "date_desc", "date_asc", "value_desc", "value_asc"
        ] = Query(default="date_desc"),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=25, ge=1, le=100),
    ) -> dict[str, Any]:
        source = request.app.state.source
        where, parameters = _where(
            query=query,
            province=province,
            department=department,
            start_date=start_date,
            end_date=end_date,
            minimum_value=minimum_value,
        )
        region_where, region_parameters = _where(
            query=query,
            province=province,
            department=department,
            start_date=start_date,
            end_date=end_date,
            minimum_value=minimum_value,
            include_province=False,
        )

        with duckdb.connect() as connection:
            summary = connection.execute(
                "SELECT count(*) AS total_records, "
                "coalesce(sum(agreement_value), 0) AS total_funding, "
                "count(DISTINCT recipient_name) AS recipient_count "
                f"FROM {source}{where}",
                parameters,
            ).fetchone()
            assert summary is not None
            total_records = int(summary[0])
            total_pages = max(1, (total_records + page_size - 1) // page_size)
            selected_page = min(page, total_pages)
            offset = (selected_page - 1) * page_size

            records = _rows(
                connection.execute(
                    f"SELECT {', '.join(RECORD_SELECT_COLUMNS)} "
                    f"FROM {source}{where} "
                    f"ORDER BY {SORT_ORDERS[sort]} LIMIT ? OFFSET ?",
                    [*parameters, page_size, offset],
                )
            )
            departments = _rows(
                connection.execute(
                    "SELECT department AS name, "
                    "coalesce(sum(agreement_value), 0) AS value, "
                    "count(*) AS count "
                    f"FROM {source}{where} GROUP BY department "
                    "ORDER BY value DESC LIMIT 6",
                    parameters,
                )
            )
            region_rows = connection.execute(
                "SELECT province, coalesce(sum(agreement_value), 0), count(*) "
                f"FROM {source}{region_where} GROUP BY province",
                region_parameters,
            ).fetchall()

        known_codes = {code for code, _ in REGIONS}
        region_totals = {
            code: {"code": code, "name": name, "value": 0.0, "count": 0}
            for code, name in REGIONS
        }
        unknown = {"name": "Unknown location", "value": 0.0, "count": 0}
        for code, value, count in region_rows:
            if code in known_codes:
                region_totals[code]["value"] = float(value)
                region_totals[code]["count"] = int(count)
            else:
                unknown["value"] += float(value)
                unknown["count"] += int(count)

        return {
            "records": records,
            "page": selected_page,
            "pageSize": page_size,
            "totalRecords": total_records,
            "totalPages": total_pages,
            "totalFunding": float(summary[1]),
            "recipientCount": int(summary[2]),
            "departments": departments,
            "regions": list(region_totals.values()),
            "unknownRegion": unknown,
            "provinceOptions": request.app.state.province_options,
            "departmentOptions": request.app.state.department_options,
            "dataset": request.app.state.dataset,
        }

    return app


app = create_app()
