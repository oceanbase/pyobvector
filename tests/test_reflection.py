import unittest
from pyobvector import *
import logging

logger = logging.getLogger(__name__)


class ObReflectionTest(unittest.TestCase):
    def test_reflection(self):
        dialect = OceanBaseDialect()
        ddl = """CREATE TABLE `embedchain_vector` (
  `id` varchar(4096) NOT NULL,
  `text` longtext DEFAULT NULL,
  `embeddings` VECTOR(1024) DEFAULT NULL,
  `metadata` json DEFAULT NULL,
  PRIMARY KEY (`id`),
  VECTOR KEY `vidx` (`embeddings`) WITH (DISTANCE=L2,M=16,EF_CONSTRUCTION=256,LIB=VSAG,TYPE=HNSW, EF_SEARCH=64) BLOCK_SIZE 16384,
  FULLTEXT KEY `idx_content_fts` (`content`) WITH PARSER ik PARSER_PROPERTIES=(ik_mode="smart") BLOCK_SIZE 16384
) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'zstd_1.3.8' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 0
"""
        state = dialect._tabledef_parser.parse(ddl, "utf8")
        assert len(state.columns) == 4
        assert len(state.keys) == 3

    def test_dialect(self):
        from sqlalchemy.dialects import registry
        from sqlalchemy.ext.asyncio import create_async_engine

        uri: str = "127.0.0.1:2881"
        user: str = "root@test"
        password: str = ""
        db_name: str = "test"
        registry.register("mysql.aoceanbase", "pyobvector", "AsyncOceanBaseDialect")
        connection_str = (
            f"mysql+aoceanbase://{user}:{password}@{uri}/{db_name}?charset=utf8mb4"
        )
        self.engine = create_async_engine(connection_str)

    def test_customized_fulltext_parser(self):
        dialect = OceanBaseDialect()
        ddl = """CREATE TABLE `t_customized_fulltext_parser` (
  `id` varchar(36) NOT NULL,
  `vector` VECTOR(768) DEFAULT NULL,
  `text` longtext DEFAULT NULL,
  `metadata` json DEFAULT NULL,
  PRIMARY KEY (`id`),
  VECTOR KEY `vector_index` (`vector`) WITH (DISTANCE=L2,M=16,EF_CONSTRUCTION=256,LIB=VSAG,TYPE=HNSW, EF_SEARCH=64) BLOCK_SIZE 16384,
  FULLTEXT KEY `fulltext_index_for_col_text` (`text`) WITH PARSER thai_ftparser BLOCK_SIZE 16384
)
"""
        state = dialect._tabledef_parser.parse(ddl, "utf8")
        assert len(state.columns) == 4
        assert len(state.keys) == 3
