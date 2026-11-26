"""XML sitemaps for Maple Atlas, generated from the live dataset.

Nginx serves these at the site root (/sitemap.xml and /sitemaps/...), because
a sitemap may only list URLs at or below its own location.

Only organization profiles are listed. Individuals' profiles are left out (and
Atlas marks them noindex) so private people's names are not pushed into search
results; placeholder entries such as "Batch report" are left out too.
"""

from __future__ import annotations

import os
from xml.sax.saxutils import escape

import duckdb

# Google's limit is 50,000 URLs per sitemap file.
URLS_PER_SITEMAP = 50_000
CONTENT_PATHS = ("/", "/how-it-works", "/methods", "/about")


def public_base_url() -> str:
    return os.environ.get("MAPLE_PUBLIC_BASE_URL", "https://atlas.zionkoudijs.com").rstrip("/")


def _organizations(source: str) -> str:
    return (
        f"SELECT DISTINCT recipient_id FROM {source} "
        "WHERE NOT recipient_is_person AND NOT recipient_is_aggregate"
    )


def recipient_sitemap_count(connection: duckdb.DuckDBPyConnection, source: str) -> int:
    total = connection.execute(f"SELECT count(*) FROM ({_organizations(source)})").fetchone()
    count = int(total[0]) if total else 0
    return max(1, -(-count // URLS_PER_SITEMAP))


def sitemap_index(files: int, lastmod: str) -> str:
    base = public_base_url()
    entries = [f"{base}/sitemaps/pages.xml"] + [
        f"{base}/sitemaps/recipients-{number}.xml" for number in range(1, files + 1)
    ]
    body = "".join(
        f"<sitemap><loc>{escape(url)}</loc><lastmod>{lastmod}</lastmod></sitemap>"
        for url in entries
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}</sitemapindex>"
    )


def _urlset(urls: list[str], lastmod: str) -> str:
    body = "".join(f"<url><loc>{escape(url)}</loc><lastmod>{lastmod}</lastmod></url>" for url in urls)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}</urlset>"
    )


def pages_sitemap(lastmod: str) -> str:
    base = public_base_url()
    return _urlset([f"{base}{path}" for path in CONTENT_PATHS], lastmod)


def recipients_sitemap(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    number: int,
    lastmod: str,
) -> str | None:
    """Recipient profile URLs for one sitemap file, in a stable order."""
    rows = connection.execute(
        f"{_organizations(source)} ORDER BY recipient_id LIMIT ? OFFSET ?",
        [URLS_PER_SITEMAP, (number - 1) * URLS_PER_SITEMAP],
    ).fetchall()
    if not rows and number > 1:
        return None
    base = public_base_url()
    return _urlset([f"{base}/recipients/{row[0]}" for row in rows], lastmod)
