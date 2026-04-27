"""MySQL / MariaDB connector.

Reads rows from a MySQL or MariaDB database via ``pymysql`` and yields one
``DocumentInfo`` per row. Uses ``DictCursor`` so every row is a plain dict
keyed by column name.

Works against regular MySQL and MariaDB.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pymysql
import pymysql.cursors
from moss import DocumentInfo


class MySQLConnector:
    """Run a SELECT against a MySQL / MariaDB database and yield one
    ``DocumentInfo`` per row.

    ``mapper`` turns a row (dict of column → value) into a ``DocumentInfo``;
    the caller decides which columns become id / text / metadata / embedding.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        query: str,
        mapper: Callable[[dict[str, Any]], DocumentInfo],
        port: int = 3306,
        charset: str = "utf8mb4",
    ) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.query = query
        self.mapper = mapper
        self.port = port
        self.charset = charset

    def __iter__(self) -> Iterator[DocumentInfo]:
        conn = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            charset=self.charset,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(self.query)
                for row in cursor:
                    yield self.mapper(row)
        finally:
            conn.close()
