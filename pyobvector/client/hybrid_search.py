"""OceanBase Hybrid Search Client."""

import json
import logging
from typing import Any

from sqlalchemy import text

from .exceptions import ClusterVersionException, ErrorCode, ExceptionsMessage
from .ob_vec_client import ObVecClient as Client
from ..util import ObVersion

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _quote_identifier(identifier: str) -> str:
    """Quote a MySQL/OceanBase identifier with backticks."""
    return "`" + identifier.replace("`", "``") + "`"


class HybridSearch(Client):
    """The OceanBase Hybrid Search Client"""

    def __init__(
        self,
        uri: str = "127.0.0.1:2881",
        user: str = "root@test",
        password: str = "",
        db_name: str = "test",
        **kwargs,
    ):
        super().__init__(uri, user, password, db_name, **kwargs)

        min_required_version = ObVersion.from_db_version_nums(4, 4, 1, 0)

        if self.ob_version < min_required_version:
            # For versions < 4.4.1.0, check if it's SeekDB
            if self._is_seekdb():
                logger.info("SeekDB detected, allowing hybrid search")
                return
            raise ClusterVersionException(
                code=ErrorCode.NOT_SUPPORTED,
                message=ExceptionsMessage.ClusterVersionIsLow
                % ("Hybrid Search", "4.4.1.0"),
            )

    def search(
        self,
        index: str,
        body: dict[str, Any],
        **kwargs,
    ):
        """Execute hybrid search with parameter compatible with Elasticsearch.

        Args:
            index: The name of the table to search
            body: The search query body
            **kwargs: Additional search parameters

        Returns:
            Search results
        """
        body_str = json.dumps(body)

        sql = text("SELECT DBMS_HYBRID_SEARCH.SEARCH(:index, :body_str)")

        with self.engine.connect() as conn:
            with conn.begin():
                res = conn.execute(
                    sql, {"index": index, "body_str": body_str}
                ).fetchone()
                if res[0] is None:
                    return []
                return json.loads(res[0])

    def get_sql(
        self,
        index: str,
        body: dict[str, Any],
    ) -> str:
        """Get the SQL actually to be executed in hybrid search.

        Args:
            index: The name of the table to search
            body: The hybrid search query body

        Returns:
            The SQL actually to be executed
        """
        body_str = json.dumps(body)

        sql = text("SELECT DBMS_HYBRID_SEARCH.GET_SQL(:index, :body_str)")

        with self.engine.connect() as conn:
            with conn.begin():
                res = conn.execute(
                    sql, {"index": index, "body_str": body_str}
                ).fetchone()
                if res[0] is None:
                    return ""
                return res[0]

    def _check_sql_search_version(self):
        min_required_version = ObVersion.from_db_version_nums(4, 6, 0, 0)
        if self.ob_version < min_required_version and not self._is_seekdb():
            raise ClusterVersionException(
                code=ErrorCode.NOT_SUPPORTED,
                message=ExceptionsMessage.ClusterVersionIsLow
                % ("Hybrid Search SQL syntax (HYBRID_SEARCH)", "4.6.0.0"),
            )

    def sql_search(
        self,
        table_name: str,
        dsl: dict[str, Any] | str,
        columns: list[str] | None = None,
        where: str | None = None,
        order_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """Execute hybrid search with the SQL-level `HYBRID_SEARCH` syntax.

        This uses the `HYBRID_SEARCH` table function introduced in OceanBase 4.6.0:
        `SELECT ... FROM HYBRID_SEARCH(TABLE table_name, DSL_STRING)`.

        Compared with `search` (based on the `DBMS_HYBRID_SEARCH` package), the
        SQL-level syntax builds a logical fusion plan at plan stage and provides
        better hybrid search performance. The DSL string is a JSON document whose
        syntax is mostly compatible with Elasticsearch, e.g.::

            {
                "query": {"match": {"content": "python javascript"}},
                "knn": {
                    "field": "vector_col",
                    "k": 5,
                    "query_vector": "[0.1, 0.2, 0.3, 0.4]",
                    "boost": 0.7,
                },
                "rank": {"rrf": {"rank_constant": 60, "rank_window_size": 10}},
                "size": 10,
            }

        Args:
            table_name: The name of the table to search. Only heap tables are
                supported (partitioned tables included).
            dsl: The hybrid search DSL, either a dict (serialized to JSON) or a
                JSON string.
            columns: Plain column names to select. If None, all columns of the
                table plus the `__score` relevance column are returned. The
                `__score` column is always included in the result, even when
                it is not listed in `columns`.
            where: Extra filter condition applied on the hybrid search result.
                OceanBase does not allow WHERE/ORDER BY/LIMIT at the same level
                as `HYBRID_SEARCH`, so the query is wrapped in a subquery
                automatically when `where` or `order_by` is set. This fragment
                is interpolated into the SQL statement verbatim: it must be a
                trusted SQL fragment and must never contain untrusted user
                input, otherwise it becomes a SQL injection risk. Prefer the
                DSL `filter` clauses for user-provided values.
            order_by: Sort expression applied on the hybrid search result, e.g.
                `id DESC` (wrapped in a subquery automatically). Like `where`,
                this fragment is interpolated verbatim and must be trusted SQL
                that never contains untrusted user input.

        Returns:
            A list of rows (dict). The relevance score of each row is in the
            `__score` field.

        Raises:
            ClusterVersionException: If the OceanBase cluster version is below
                4.6.0.0.
        """
        self._check_sql_search_version()

        if isinstance(dsl, dict):
            dsl_str = json.dumps(dsl, ensure_ascii=False)
        else:
            dsl_str = dsl

        if columns is None:
            col_expr = "*"
        else:
            # `HYBRID_SEARCH` always produces a `__score` relevance column.
            # Keep it in the projection so that every returned row carries
            # its score even when a column subset is requested.
            selected = [_quote_identifier(c) for c in columns]
            if all(c.lower() != "__score" for c in columns):
                selected.append(_quote_identifier("__score"))
            col_expr = ", ".join(selected)
        hybrid_search_from = (
            f"HYBRID_SEARCH(TABLE {_quote_identifier(table_name)}, :dsl)"
        )

        if where is None and order_by is None:
            stmt = f"SELECT {col_expr} FROM {hybrid_search_from}"
        else:
            stmt = (
                f"SELECT {col_expr} FROM "
                f"(SELECT * FROM {hybrid_search_from}) AS __hybrid_search_result"
            )
            if where is not None:
                stmt += f" WHERE {where}"
            if order_by is not None:
                stmt += f" ORDER BY {order_by}"

        with self.engine.connect() as conn:
            with conn.begin():
                res = conn.execute(text(stmt), {"dsl": dsl_str})
                return [dict(row) for row in res.mappings().fetchall()]
