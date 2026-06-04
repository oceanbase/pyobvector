"""
Build a SQLAlchemy Engine from pyseekdb embedded client so ObClient/ObVecClient
work the same for both remote and embedded SeekDB.

Requires optional dependency: pip install pyobvector[pyseekdb]
"""

import logging
import re
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

logger = logging.getLogger(__name__)

_QUERY_SQL_PREFIXES = ("SELECT", "SHOW", "DESCRIBE", "DESC")


def _is_query_sql(sql: str) -> bool:
    """Return True if sql is a row-returning statement (SELECT/SHOW/DESCRIBE/DESC)."""
    return sql.strip().upper().startswith(_QUERY_SQL_PREFIXES)


def _description_from_select(sql: str) -> list[tuple]:
    """Extract DBAPI description tuples from SELECT column list using sqlglot.

    Falls back to [] when sqlglot cannot parse the statement (e.g. SHOW/DESCRIBE).
    """
    try:
        import sqlglot
        import sqlglot.expressions as exp

        parsed = sqlglot.parse_one(sql)
        if parsed is None or not hasattr(parsed, "selects"):
            return []
        cols = []
        for sel in parsed.selects:
            name = sel.alias or (sel.name if isinstance(sel, exp.Column) else sel.sql())
            cols.append(name)
        return [(c, None, None, None, None, None, None) for c in cols]
    except Exception as e:
        logger.debug("_description_from_select could not parse SQL: %s", e)
        return []


def _pyformat_to_format(sql: str, params: Any) -> tuple[str, list[Any]]:
    """Convert SQLAlchemy pyformat (%(name)s) + dict params to %s + list for pyseekdb."""
    if not isinstance(params, Mapping):
        return sql, list(params) if params is not None else []

    # Find placeholder names in order: %(name)s
    pattern = re.compile(r"%\(([^)]+)\)s")
    names = pattern.findall(sql)
    if not names:
        return sql, []

    values = [params[n] for n in names]
    new_sql = pattern.sub("%s", sql)
    return new_sql, values


def _execute_via_pyseekdb(client: Any, sql: str, params: Any) -> list[dict[str, Any]]:
    """Execute SQL via pyseekdb SeekdbEmbeddedClient; accepts dict or list params."""
    sql, param_list = _pyformat_to_format(sql, params)
    conn = client.get_raw_connection()
    return client._execute_query_with_cursor(
        conn, sql, param_list, use_context_manager=False
    )


class _SeekdbCursor:
    """DBAPI-2 style Cursor delegating to pyseekdb SeekdbEmbeddedClient."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._description: list[tuple[str]] | None = None
        self._rows: list[tuple] | None = None
        self.rowcount = -1

    def execute(self, operation: str, parameters: Sequence[Any] | None = None) -> None:
        result = _execute_via_pyseekdb(self._client, operation, parameters or ())
        if not result:
            # For SELECT/SHOW/DESCRIBE: populate description so SQLAlchemy treats this
            # as a row-returning result with 0 rows rather than a non-returning statement.
            # _NoResultMetaData (from None) would cause ResourceClosedError on fetchall().
            self._description = (
                _description_from_select(operation)
                if _is_query_sql(operation)
                else None
            )
            self._rows = []
            self.rowcount = 0
            return

        def make_desc(name: str) -> tuple:
            return (name, None, None, None, None, None, None)

        first = result[0]
        if isinstance(first, dict):
            keys = list(first.keys())
            self._description = [make_desc(k) for k in keys]
            self._rows = [tuple(row[k] for k in keys) for row in result]
        else:
            n = len(first)
            self._description = [make_desc(f"column_{i}") for i in range(n)]
            self._rows = [
                tuple(row) if not isinstance(row, tuple) else row for row in result
            ]
        self.rowcount = len(self._rows)

    def fetchall(self) -> list[tuple]:
        return self._rows or []

    def fetchone(self) -> tuple | None:
        if not self._rows:
            return None
        return self._rows.pop(0)

    @property
    def description(self) -> list[tuple[str]] | None:
        return self._description

    def close(self) -> None:
        # Rows are already buffered in memory by execute(); nothing to release here.
        # Clearing _rows/_description would break callers that fetch results after
        # SQLAlchemy closes the cursor on exiting the connection context block.
        pass


class _SeekdbConnection:
    """DBAPI-2 style Connection holding a pyseekdb SeekdbEmbeddedClient."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def cursor(self) -> _SeekdbCursor:
        return _SeekdbCursor(self._client)

    def close(self) -> None:
        if hasattr(self._client, "_cleanup"):
            self._client._cleanup()

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def character_set_name(self) -> str:
        return "utf8mb4"


def create_engine_from_client(pyseekdb_client: Any, **kwargs: Any):
    """
    Create a SQLAlchemy Engine from an existing pyseekdb.Client.

    Use when you have client = pyseekdb.Client(path=..., database=...).
    """
    from sqlalchemy.dialects import registry

    registry.register(
        "mysql.oceanbase", "pyobvector.schema.dialect", "OceanBaseDialect"
    )
    server = getattr(pyseekdb_client, "_server", None)
    if server is None:
        raise ValueError(
            "pyseekdb_client must be a pyseekdb.Client instance (has _server). "
            "Create with: pyseekdb.Client(path='./seekdb.db', database='test')"
        )
    database = getattr(server, "database", "test")

    def creator() -> _SeekdbConnection:
        return _SeekdbConnection(server)

    engine = create_engine(
        "mysql+oceanbase://root:@127.0.0.1:2881/" + database,
        creator=creator,
        poolclass=NullPool,
        **kwargs,
    )
    engine.update_execution_options(seekdb_server=server)
    return engine


def create_embedded_engine(path: str, database: str = "test", **kwargs: Any):
    """
    Create a SQLAlchemy Engine from embedded SeekDB using official pyseekdb.Client().
    """
    try:
        import pyseekdb
    except ImportError as e:
        raise ImportError(
            "Embedded SeekDB requires: pip install pyobvector[pyseekdb]"
        ) from e

    client = pyseekdb.Client(path=path, database=database)
    return create_engine_from_client(client, **kwargs)
