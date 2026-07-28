import logging
import unittest

from sqlalchemy import Column, Integer, VARCHAR

from pyobvector import VECTOR, VectorIndex, FtsIndexParam, FtsParser
from pyobvector.client.hybrid_search import HybridSearch
from pyobvector.util import ObVersion

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class HybridSearchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = HybridSearch()

    def _skip_if_sql_search_not_supported(self):
        if (
            self.client.ob_version < ObVersion.from_db_version_nums(4, 6, 0, 0)
            and not self.client._is_seekdb()
        ):
            self.skipTest("HYBRID_SEARCH SQL syntax requires OceanBase >= 4.6.0.0")

    def _create_test_table(self, test_table_name: str):
        self.client.create_table(
            table_name=test_table_name,
            columns=[
                Column("id", Integer, primary_key=True, autoincrement=False),
                Column("source_id", VARCHAR(32)),
                Column("enabled", Integer),
                Column("vector", VECTOR(3)),
                Column("title", VARCHAR(255)),
                Column("content", VARCHAR(255)),
            ],
            indexes=[
                VectorIndex(
                    "vec_idx", "vector", params="distance=l2, type=hnsw, lib=vsag"
                ),
            ],
            mysql_charset="utf8mb4",
            mysql_collate="utf8mb4_unicode_ci",
            mysql_organization="heap",
        )

        for col in ["title", "content"]:
            self.client.create_fts_idx_with_fts_index_param(
                table_name=test_table_name,
                fts_idx_param=FtsIndexParam(
                    index_name=f"fts_idx_{col}",
                    field_names=[col],
                    parser_type=FtsParser.IK,
                ),
            )

        self.client.insert(
            table_name=test_table_name,
            data=[
                {
                    "id": 1,
                    "source_id": "3b767712b57211f09c170242ac130008",
                    "enabled": 1,
                    "vector": [1, 1, 1],
                    "title": "Differences between enterprise and community editions",
                    "content": "OceanBase database provides both enterprise and community editions.",
                },
                {
                    "id": 2,
                    "vector": [1, 2, 3],
                    "enabled": 1,
                    "source_id": "3b791472b57211f09c170242ac130008",
                    "title": "Quick start with OceanBase community edition",
                    "content": "This article introduces how to quickly deploy the OceanBase database in different scenarios, helping you get started with the OceanBase database quickly.",
                },
                {
                    "id": 3,
                    "source_id": "3b7af31eb57211f09c170242ac130008",
                    "enabled": 1,
                    "vector": [3, 2, 1],
                    "title": "Configuration best practices",
                    "content": "To ensure good performance across various business scenarios, OceanBase summarizes recommended settings for core configuration items and variables based on tuning experience from real-world scenarios.",
                },
                {
                    "id": 4,
                    "source_id": "3b7cb9ceb57211f09c170242ac130008",
                    "enabled": 1,
                    "vector": [2, 2, 2],
                    "title": "OceanBase real-time analytics white paper",
                    "content": "An in-depth interpretation of the 8 core features of OceanBase real-time analytics, with practices and cases in HTAP hybrid workloads, real-time data analysis, and PL/SQL batch processing.",
                },
            ],
        )

    def _search_param(self):
        query = {
            "bool": {
                "must": [
                    {
                        "query_string": {
                            "fields": ["title^10", "content"],
                            "type": "best_fields",
                            "query": '((database)^0.5106318299637825 (migration)^0.2651122588583924 (oceanbase)^0.22425591117782506 ("oceanbase database migration"~2)^1.5)',
                            "minimum_should_match": "30%",
                            "boost": 1,
                        }
                    }
                ],
                "filter": [
                    {
                        "terms": {
                            "source_id": [
                                "3b791472b57211f09c170242ac130008",
                                "3b7af31eb57211f09c170242ac130008",
                            ]
                        }
                    },
                    {"bool": {"must_not": [{"range": {"enabled": {"lt": 1}}}]}},
                ],
                "boost": 0.7,
            }
        }

        return {
            "query": query,
            "knn": {
                "field": "vector",
                "k": 1024,
                "num_candidates": 1024,
                "query_vector": [1, 2, 3],
                "filter": query,
                "similarity": 0.2,
            },
            "from": 0,
            "size": 60,
        }

    def test_search(self):
        test_table_name = "hybrid_search_test"
        self._create_test_table(test_table_name)
        body = self._search_param()

        res = self.client.search(index=test_table_name, body=body)
        assert isinstance(res, list)
        assert len(res) > 0

    def test_get_sql(self):
        test_table_name = "get_sql_test"
        self._create_test_table(test_table_name)
        body = self._search_param()

        sql = self.client.get_sql(index=test_table_name, body=body)
        res = self.client.perform_raw_text_sql(sql).fetchall()
        assert len(res) > 0

    def test_sql_search_knn(self):
        self._skip_if_sql_search_not_supported()
        test_table_name = "sql_search_knn_test"
        self._create_test_table(test_table_name)

        res = self.client.sql_search(
            table_name=test_table_name,
            dsl={
                "knn": {
                    "field": "vector",
                    "k": 2,
                    "query_vector": "[1, 2, 3]",
                }
            },
        )
        assert len(res) == 2
        assert res[0]["id"] == 2
        assert "__score" in res[0]

    def test_sql_search_match(self):
        self._skip_if_sql_search_not_supported()
        test_table_name = "sql_search_match_test"
        self._create_test_table(test_table_name)

        res = self.client.sql_search(
            table_name=test_table_name,
            dsl={"query": {"match": {"content": "OceanBase database"}}},
            columns=["id", "title"],
        )
        assert len(res) > 0
        assert set(res[0].keys()) == {"id", "title", "__score"}

    def test_sql_search_hybrid_with_rrf(self):
        self._skip_if_sql_search_not_supported()
        test_table_name = "sql_search_hybrid_test"
        self._create_test_table(test_table_name)

        res = self.client.sql_search(
            table_name=test_table_name,
            dsl={
                "query": {
                    "match": {"content": {"query": "OceanBase database", "boost": 0.3}}
                },
                "knn": {
                    "field": "vector",
                    "k": 4,
                    "query_vector": "[1, 2, 3]",
                    "boost": 0.7,
                },
                "rank": {
                    "rrf": {
                        "rank_constant": 60,
                        "rank_window_size": 10,
                    }
                },
                "size": 10,
            },
        )
        assert len(res) > 0
        assert "__score" in res[0]

    def test_sql_search_multi_knn(self):
        self._skip_if_sql_search_not_supported()
        test_table_name = "sql_search_multi_knn_test"
        self._create_test_table(test_table_name)

        res = self.client.sql_search(
            table_name=test_table_name,
            dsl={
                "knn": [
                    {"field": "vector", "k": 2, "query_vector": "[1, 2, 3]"},
                    {"field": "vector", "k": 2, "query_vector": "[1, 1, 1]"},
                ]
            },
        )
        assert len(res) > 0
        assert "__score" in res[0]

    def test_sql_search_with_filter_and_outer_where(self):
        self._skip_if_sql_search_not_supported()
        test_table_name = "sql_search_filter_test"
        self._create_test_table(test_table_name)

        res = self.client.sql_search(
            table_name=test_table_name,
            dsl={
                "knn": {
                    "field": "vector",
                    "k": 4,
                    "query_vector": "[1, 2, 3]",
                    "filter": [{"range": {"id": {"gte": 2}}}],
                }
            },
            where="enabled = 1",
            order_by="id DESC",
        )
        assert len(res) > 0
        ids = [row["id"] for row in res]
        assert all(i >= 2 for i in ids)
        assert ids == sorted(ids, reverse=True)
