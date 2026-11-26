"""Recipient profiles: everything one organization or person has received.

All figures use the latest amendment of each agreement (the ``source``
passed in), so amended agreements are counted once.
"""

from __future__ import annotations

from typing import Any

import duckdb

RECIPIENT_COLUMNS = frozenset(
    {"recipient_id", "recipient_is_person", "recipient_is_aggregate", "recipient_type"}
)

RECIPIENT_TYPES = {
    "A": "Indigenous recipient",
    "F": "For-profit organization",
    "G": "Government",
    "I": "International organization",
    "N": "Not-for-profit organization or charity",
    "O": "Other",
    "P": "Individual",
    "S": "Academic institution",
}

PROFILE_AGREEMENT_COLUMNS = (
    "record_id",
    "agreement_id",
    "agreement_title",
    "agreement_value",
    "agreement_start_date",
    "agreement_end_date",
    "department",
    "program_name",
    "description",
    "amendment_number",
    "amendment_count",
)


def _rows(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _totals(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    recipient_id: str,
    column: str,
    limit: int,
) -> list[dict[str, Any]]:
    return _rows(
        connection.execute(
            f"SELECT {column} AS name, sum(agreement_value) AS value, count(*) AS count "
            f"FROM {source} WHERE recipient_id = ? AND {column} IS NOT NULL "
            "GROUP BY 1 ORDER BY value DESC, name LIMIT ?",
            [recipient_id, limit],
        )
    )


def _affiliation(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    recipient_id: str,
) -> dict[str, Any] | None:
    """The organization whose business number a person's grants are filed under.

    Researchers and students are published under their institution's BN, so
    this is evidence from the government data itself, not from the web.
    """
    row = connection.execute(
        "WITH numbers AS ("
        f"  SELECT business_number, count(*) AS uses FROM {source} "
        "  WHERE recipient_id = ? AND business_number IS NOT NULL GROUP BY 1"
        "), owners AS ("
        "  SELECT g.business_number, g.recipient_name, count(*) AS uses "
        f"  FROM (SELECT * FROM {source}) AS g JOIN numbers USING (business_number) "
        "  WHERE NOT g.recipient_is_person GROUP BY 1, 2"
        ") "
        "SELECT owners.recipient_name, owners.business_number, numbers.uses "
        "FROM owners JOIN numbers USING (business_number) "
        "ORDER BY numbers.uses DESC, owners.uses DESC, owners.recipient_name LIMIT 1",
        [recipient_id],
    ).fetchone()
    if row is None:
        return None
    return {"name": row[0], "businessNumber": row[1], "agreementCount": int(row[2])}


def recipient_profile(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    recipient_id: str,
) -> dict[str, Any] | None:
    summary = connection.execute(
        "SELECT count(*), sum(agreement_value), min(agreement_start_date), "
        "max(agreement_start_date), bool_or(recipient_is_person), "
        "mode(recipient_type), mode(business_number), bool_or(recipient_is_aggregate) "
        f"FROM {source} WHERE recipient_id = ?",
        [recipient_id],
    ).fetchone()
    if summary is None or not summary[0]:
        return None
    count, total, first_start, last_start, is_person, kind, business_number, aggregate = summary

    by_year = _rows(
        connection.execute(
            "SELECT year(agreement_start_date) AS year, sum(agreement_value) AS value, "
            "count(*) AS count "
            f"FROM {source} WHERE recipient_id = ? AND agreement_start_date IS NOT NULL "
            "GROUP BY 1 ORDER BY 1",
            [recipient_id],
        )
    )
    undated = connection.execute(
        "SELECT coalesce(sum(agreement_value), 0), count(*) "
        f"FROM {source} WHERE recipient_id = ? AND agreement_start_date IS NULL",
        [recipient_id],
    ).fetchone()
    assert undated is not None
    largest = _rows(
        connection.execute(
            f"SELECT {', '.join(PROFILE_AGREEMENT_COLUMNS)} FROM {source} "
            "WHERE recipient_id = ? "
            "ORDER BY agreement_value DESC, record_id LIMIT 10",
            [recipient_id],
        )
    )
    # The most frequently published spelling is the display name.
    names = [
        row[0]
        for row in connection.execute(
            f"SELECT recipient_name FROM {source} WHERE recipient_id = ? "
            "GROUP BY 1 ORDER BY count(*) DESC, recipient_name LIMIT 5",
            [recipient_id],
        ).fetchall()
    ]
    locations = _rows(
        connection.execute(
            "SELECT city, province, count(*) AS count "
            f"FROM {source} WHERE recipient_id = ? "
            "AND (city IS NOT NULL OR province IS NOT NULL) "
            "GROUP BY 1, 2 ORDER BY count DESC, city LIMIT 5",
            [recipient_id],
        )
    )

    return {
        "recipientId": recipient_id,
        "name": names[0],
        "otherNames": names[1:],
        "isPerson": bool(is_person),
        # Placeholder rows ("Batch report", "Withheld") are not one recipient.
        "isAggregate": bool(aggregate),
        "recipientType": kind,
        "recipientTypeLabel": RECIPIENT_TYPES.get(kind) if kind else None,
        # A person's business number belongs to their institution, not them.
        "businessNumber": None if is_person else business_number,
        "affiliation": (
            _affiliation(connection, source, recipient_id) if is_person else None
        ),
        "locations": locations,
        "totalFunding": float(total or 0),
        "agreementCount": int(count),
        "firstAgreementStart": first_start.isoformat() if first_start else None,
        "lastAgreementStart": last_start.isoformat() if last_start else None,
        "byYear": by_year,
        "undated": {"value": float(undated[0]), "count": int(undated[1])},
        "departments": _totals(connection, source, recipient_id, "department", 8),
        "programs": _totals(connection, source, recipient_id, "program_name", 8),
        "largestAgreements": largest,
    }


def top_recipients(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    *,
    limit: int,
    include_people: bool,
) -> list[dict[str, Any]]:
    people_filter = (
        "WHERE NOT recipient_is_aggregate "
        if include_people
        else "WHERE NOT recipient_is_aggregate AND NOT recipient_is_person "
    )
    return _rows(
        connection.execute(
            "SELECT recipient_id AS \"recipientId\", mode(recipient_name) AS name, "
            "bool_or(recipient_is_person) AS \"isPerson\", "
            "sum(agreement_value) AS \"totalFunding\", count(*) AS \"agreementCount\" "
            f"FROM {source} {people_filter}"
            "GROUP BY recipient_id ORDER BY \"totalFunding\" DESC, recipient_id LIMIT ?",
            [limit],
        )
    )
