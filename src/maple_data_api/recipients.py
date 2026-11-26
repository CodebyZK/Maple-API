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


SIZE_BUCKETS = (
    ("Under $25K", None, 25_000),
    ("$25K to $100K", 25_000, 100_000),
    ("$100K to $1M", 100_000, 1_000_000),
    ("$1M to $10M", 1_000_000, 10_000_000),
    ("$10M and over", 10_000_000, None),
)


def _size_buckets(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    recipient_id: str,
) -> list[dict[str, Any]]:
    """How many agreements fall in each size range, and their total value."""
    cases = " ".join(
        f"WHEN {'TRUE' if low is None else f'agreement_value >= {low}'} "
        f"AND {'TRUE' if high is None else f'agreement_value < {high}'} THEN {index}"
        for index, (_, low, high) in enumerate(SIZE_BUCKETS)
    )
    rows = dict(
        (int(index), (int(count), float(value or 0)))
        for index, count, value in connection.execute(
            f"SELECT CASE {cases} END AS bucket, count(*), sum(agreement_value) "
            f"FROM {source} WHERE recipient_id = ? GROUP BY 1",
            [recipient_id],
        ).fetchall()
        if index is not None
    )
    return [
        {"label": label, "count": rows.get(index, (0, 0.0))[0], "value": rows.get(index, (0, 0.0))[1]}
        for index, (label, _, _) in enumerate(SIZE_BUCKETS)
    ]


def _program_rank(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    recipient_id: str,
    program: str,
) -> dict[str, Any] | None:
    """Where the recipient ranks among everyone funded by its main program."""
    row = connection.execute(
        "WITH totals AS ("
        f"  SELECT recipient_id, sum(agreement_value) AS value FROM {source} "
        "  WHERE program_name = ? AND NOT recipient_is_aggregate GROUP BY 1"
        "), ranked AS ("
        "  SELECT recipient_id, value, rank() OVER (ORDER BY value DESC) AS position FROM totals"
        ") "
        "SELECT (SELECT position FROM ranked WHERE recipient_id = ?), count(*), sum(value), "
        "(SELECT value FROM ranked WHERE recipient_id = ?) FROM ranked",
        [program, recipient_id, recipient_id],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    position, recipients, program_total, value = row
    return {
        "program": program,
        "rank": int(position),
        "recipients": int(recipients),
        "programTotal": float(program_total or 0),
        "share": float(value or 0) / float(program_total) if program_total else 0.0,
    }


def similar_recipients(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    recipient_id: str,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """Organizations funded by the same programs, most overlap first.

    Similarity is computed from the government data only: recipients sharing
    more of this recipient's top programs, then more funding in them, rank
    higher. Individuals get no similar list, to avoid profiling people.
    """
    rows = connection.execute(
        "WITH mine AS ("
        f"  SELECT program_name, sum(agreement_value) AS value FROM {source} "
        "  WHERE recipient_id = ? AND program_name IS NOT NULL "
        "  GROUP BY 1 ORDER BY value DESC LIMIT 5"
        "), me AS ("
        f"  SELECT bool_or(recipient_is_person) AS person FROM {source} WHERE recipient_id = ?"
        "), shared AS ("
        "  SELECT g.recipient_id, g.program_name, sum(g.agreement_value) AS value "
        f"  FROM (SELECT * FROM {source}) AS g JOIN mine USING (program_name) "
        "  WHERE g.recipient_id <> ? AND NOT g.recipient_is_aggregate "
        "    AND NOT g.recipient_is_person AND NOT (SELECT person FROM me) "
        "  GROUP BY 1, 2"
        ") "
        "SELECT recipient_id, count(*) AS shared_programs, sum(value) AS shared_value, "
        "list(program_name ORDER BY value DESC) AS programs "
        "FROM shared GROUP BY 1 ORDER BY shared_programs DESC, shared_value DESC LIMIT ?",
        # Fetch extra rows: one organization can have several profiles under
        # name variants, and each name is listed once.
        [recipient_id, recipient_id, recipient_id, limit * 2],
    ).fetchall()
    if not rows:
        return []
    ids = [row[0] for row in rows]
    placeholders = ", ".join("?" for _ in ids)
    details = {
        row[0]: row[1:]
        for row in connection.execute(
            "SELECT recipient_id, mode(recipient_name), mode(province), mode(recipient_type), "
            "sum(agreement_value), count(*) "
            f"FROM {source} WHERE recipient_id IN ({placeholders}) GROUP BY 1",
            ids,
        ).fetchall()
    }
    similar = []
    seen_names: set[str] = set()
    for other_id, shared_programs, shared_value, programs in rows:
        name, province, kind, total, count = details[other_id]
        key = " ".join(str(name).split("|", 1)[0].casefold().split())
        if key in seen_names or len(similar) >= limit:
            continue
        seen_names.add(key)
        similar.append(
            {
                "recipientId": other_id,
                "name": name,
                "province": province,
                "recipientTypeLabel": RECIPIENT_TYPES.get(kind) if kind else None,
                "sharedPrograms": list(programs),
                "sharedValue": float(shared_value or 0),
                "totalFunding": float(total or 0),
                "agreementCount": int(count),
            }
        )
    return similar


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
    # The most frequently published spelling is the display name; on a tie,
    # prefer a mixed-case spelling over ALL CAPS.
    names = [
        row[0]
        for row in connection.execute(
            f"SELECT recipient_name FROM {source} WHERE recipient_id = ? "
            "GROUP BY 1 ORDER BY count(*) DESC, "
            "(recipient_name = upper(recipient_name)) ASC, recipient_name LIMIT 5",
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

    programs = _totals(connection, source, recipient_id, "program_name", 8)

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
        "programs": programs,
        "largestAgreements": largest,
        "sizeBuckets": _size_buckets(connection, source, recipient_id),
        "programRank": (
            _program_rank(connection, source, recipient_id, programs[0]["name"])
            if programs and not aggregate
            else None
        ),
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


CHARITY_AMOUNTS = (
    "revenue_federal",
    "revenue_provincial",
    "revenue_municipal",
    "revenue_government_total",
    "total_revenue",
    "charitable_programs",
    "management_and_administration",
    "fundraising",
    "total_expenditures",
    "total_assets",
    "total_liabilities",
)


def charity_for(
    connection: duckdb.DuckDBPyConnection,
    charities_source: str,
    recipient_id: str,
) -> dict[str, Any] | None:
    """The CRA charity return linked to a recipient, if it is a registered charity.

    Amounts that the charity did not report stay null; they are never zero.
    """
    row = connection.execute(
        "SELECT registration_number, legal_name, designation, category, city, province, "
        "fiscal_period_end, section_used, list_year, "
        + ", ".join(CHARITY_AMOUNTS)
        # An organization can hold several registrations under one business
        # number; show the largest by revenue so the choice is stable.
        + f" FROM {charities_source} WHERE recipient_id = ? "
        "ORDER BY total_revenue DESC NULLS LAST, registration_number LIMIT 1",
        [recipient_id],
    ).fetchone()
    if row is None:
        return None
    keys = (
        "registrationNumber", "legalName", "designation", "category", "city", "province",
        "fiscalPeriodEnd", "sectionUsed", "listYear",
    )
    charity: dict[str, Any] = dict(zip(keys, row[: len(keys)], strict=True))
    if charity["fiscalPeriodEnd"] is not None:
        charity["fiscalPeriodEnd"] = charity["fiscalPeriodEnd"].isoformat()
    charity["amounts"] = {
        name: (float(value) if value is not None else None)
        for name, value in zip(CHARITY_AMOUNTS, row[len(keys):], strict=True)
    }
    charity["source"] = "Canada Revenue Agency, List of charities"
    charity["sourceUrl"] = (
        "https://open.canada.ca/data/en/dataset/80c00cdb-1358-415c-bb8b-0de7f12675b8"
    )
    return charity
