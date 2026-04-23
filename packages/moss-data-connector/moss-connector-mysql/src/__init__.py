"""MySQL / MariaDB source connector for Moss.

    from moss_connector_mysql import MySQLConnector, ingest
"""

from .connector import MySQLConnector
from .ingest import ingest

__all__ = ["MySQLConnector", "ingest"]
