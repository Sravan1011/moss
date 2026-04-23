# moss-connector-mysql

MySQL / MariaDB source connector for Moss. Uses [PyMySQL](https://github.com/PyMySQL/PyMySQL) so it works against regular MySQL, MariaDB, and PlanetScale.

## Install

```bash
pip install moss-connector-mysql
```

This installs `pymysql` automatically.

## Usage

```python
import asyncio
from moss import DocumentInfo
from moss_connector_mysql import MySQLConnector, ingest

async def main():
    source = MySQLConnector(
        host="localhost",
        user="root",
        password="secret",
        database="mydb",
        query="SELECT id, title, body FROM articles",
        mapper=lambda row: DocumentInfo(
            id=str(row["id"]),
            text=row["body"],
            metadata={"title": row["title"]},
        ),
        port=3306,
    )

    result = await ingest(
        source,
        project_id="your_project_id",
        project_key="your_project_key",
        index_name="articles",
    )
    print(f"copied {result.doc_count} rows")

asyncio.run(main())
```

## Layout

```
src/
├── __init__.py      # re-exports MySQLConnector and ingest
├── connector.py     # MySQLConnector class
└── ingest.py        # ingest() - keep in sync with the other connector packages
```

## Tests

```bash
pip install -e ".[dev]"
pytest tests/test_mysql.py -v                         # mocked, no network or DB needed
pytest tests/test_integration_mysql_moss.py -v -s     # live MySQL + Moss (requires MYSQL_URL, MOSS_PROJECT_ID, MOSS_PROJECT_KEY)
```
