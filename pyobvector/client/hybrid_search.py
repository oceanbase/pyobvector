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
                message=ExceptionsMessage.ClusterVersionIsLow % ("Hybrid Search", "4.4.1.0"),
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
                res = conn.execute(sql, {"index": index, "body_str": body_str}).fetchone()
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
                res = conn.execute(sql, {"index": index, "body_str": body_str}).fetchone()
                return res[0]
