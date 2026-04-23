"""End-to-end integration test against a real MySQL database and Moss project.

This test actually creates a temporary table in MySQL, ingests from it into a
real Moss index, queries the index, and cleans up afterwards. It is SKIPPED
unless MYSQL_URL, MOSS_PROJECT_ID, and MOSS_PROJECT_KEY are all set in the
environment (or in a .env file at the repo root or package root).

MYSQL_URL should be a connection string like:
    mysql://user:password@host:port/database

Run it with:
    cd packages/moss-data-connector/moss-connector-mysql
    pytest tests/test_integration_mysql_moss.py -v -s
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from urllib.parse import urlparse

import pytest

# Load .env from the package dir, then the repo root, if present.
try:
    from dotenv import load_dotenv

    _here = Path(__file__).resolve()
    for candidate in (
        _here.parents[1] / ".env",                   # this package's own .env
        _here.parents[2] / ".env",                   # moss-data-connector/.env
        _here.parents[4] / ".env",                   # <repo>/.env
    ):
        if candidate.exists():
            load_dotenv(candidate, override=False)
except ImportError:
    pass  # dotenv is optional; env vars can also be set directly.

pytest.importorskip("pymysql")

import pymysql  # noqa: E402
import pymysql.cursors  # noqa: E402
from moss import DocumentInfo, MossClient, QueryOptions  # noqa: E402
from moss_connector_mysql import MySQLConnector, ingest  # noqa: E402

MYSQL_URL = os.getenv("MYSQL_URL")
PROJECT_ID = os.getenv("MOSS_PROJECT_ID")
PROJECT_KEY = os.getenv("MOSS_PROJECT_KEY")

pytestmark = pytest.mark.skipif(
    not (MYSQL_URL and PROJECT_ID and PROJECT_KEY),
    reason="Set MYSQL_URL, MOSS_PROJECT_ID, and MOSS_PROJECT_KEY to run the real integration test.",
)


def _parse_mysql_url(url: str) -> dict:
    """Parse a mysql://user:pass@host:port/db URL into pymysql.connect kwargs."""
    parsed = urlparse(url)
    return {
        "host": parsed.hostname or "localhost",
        "user": parsed.username or "root",
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/"),
        "port": parsed.port or 3306,
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }


@pytest.fixture()
def mysql_table():
    """Create a temporary table with 5 recognisable rows, drop it after the test."""
    params = _parse_mysql_url(MYSQL_URL)
    table_name = f"moss_test_{uuid.uuid4().hex[:8]}"
    conn = pymysql.connect(**params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {table_name} "
                "(id INT PRIMARY KEY, title VARCHAR(255), body TEXT)"
            )
            cur.executemany(
                f"INSERT INTO {table_name} (id, title, body) VALUES (%s, %s, %s)",
                [
                    (1, "Refund policy", "Refunds take 3 to 5 business days."),
                    (2, "Shipping time", "Orders ship within 24 hours."),
                    (3, "Contact support", "Reach support 24/7 via live chat."),
                    (4, "Password reset", "Click the link on the login page."),
                    (5, "Order tracking", "Tracking number sent by email."),
                ],
            )
        conn.commit()
        yield table_name, params
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        conn.close()


async def test_mysql_ingest_end_to_end(mysql_table):
    """Full round trip: MySQL → Moss index → query → delete."""
    table_name, params = mysql_table
    client = MossClient(PROJECT_ID, PROJECT_KEY)

    # Unique index name per run so concurrent runs don't collide.
    index_name = f"moss-mysql-e2e-{uuid.uuid4().hex[:8]}"

    try:
        connector = MySQLConnector(
            host=params["host"],
            user=params["user"],
            password=params["password"],
            database=params["database"],
            query=f"SELECT id, title, body FROM {table_name}",
            mapper=lambda r: DocumentInfo(
                id=str(r["id"]),
                text=r["body"],
                metadata={"title": r["title"]},
            ),
            port=params["port"],
        )

        result = await ingest(connector, PROJECT_ID, PROJECT_KEY, index_name=index_name)
        assert result is not None
        assert result.doc_count == 5

        # Query the live index. "refund" should pull back article 1.
        await client.load_index(index_name)
        result = await client.query(
            index_name, "how long do refunds take", QueryOptions(top_k=3)
        )

        assert result.docs, "expected at least one document in the search result"
        top_ids = [d.id for d in result.docs]
        assert "1" in top_ids, f"refund-policy doc not in top 3: {top_ids}"

        # Check the metadata survived the round trip.
        refund_doc = next(d for d in result.docs if d.id == "1")
        assert refund_doc.metadata is not None
        assert refund_doc.metadata.get("title") == "Refund policy"

    finally:
        # Always try to clean up, even if an assertion above failed.
        try:
            await client.delete_index(index_name)
        except Exception as exc:  # pragma: no cover, best-effort cleanup
            print(f"warning: failed to delete test index {index_name}: {exc}")
