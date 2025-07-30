import unittest
from pyobvector import *
import logging
from unittest.mock import Mock, patch
from pyobvector.schema.reflection import OceanBaseTableDefinitionParser

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
        dialect._tabledef_parser.parse(ddl, "utf8")

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

    def test_parse_constraints_with_string_spec(self):
        """Test that _parse_constraints handles string spec gracefully without crashing."""
        # Create a parser instance
        dialect = OceanBaseDialect()
        parser = OceanBaseTableDefinitionParser(dialect, dialect.preparer, default_schema="test")
        
        # Mock the parent class _parse_constraints to return different types of spec
        test_cases = [
            # Case 1: spec is a string (this was causing the bug)
            ("fk_constraint", "some_string_spec"),
            # Case 2: spec is a dict with onupdate/ondelete = "restrict" 
            ("fk_constraint", {
                "table": ["test", "other_table"],
                "onupdate": "restrict", 
                "ondelete": "restrict"
            }),
            # Case 3: spec is a dict with onupdate/ondelete = "cascade"
            ("fk_constraint", {
                "table": ["other_table"],
                "onupdate": "cascade",
                "ondelete": "cascade"
            }),
            # Case 4: spec is a dict without onupdate/ondelete
            ("fk_constraint", {
                "table": ["other_table"],
                "name": "fk_test"
            }),
            # Case 5: spec is None (edge case)
            ("fk_constraint", None),
        ]
        
        for tp, spec in test_cases:
            with self.subTest(tp=tp, spec=spec):
                # Mock the parent class method to return our test case
                with patch.object(parser.__class__.__bases__[0], '_parse_constraints', return_value=(tp, spec)):
                    # This should not raise an exception
                    result = parser._parse_constraints("dummy line")
                    
                    # Verify the result
                    if result:
                        result_tp, result_spec = result
                        self.assertEqual(result_tp, tp)
                        
                        # If spec was a dict with "restrict" values, they should be None now
                        if isinstance(spec, dict):
                            if spec.get("onupdate") == "restrict":
                                self.assertIsNone(result_spec.get("onupdate"))
                            if spec.get("ondelete") == "restrict":
                                self.assertIsNone(result_spec.get("ondelete"))
                        else:
                            # If spec was not a dict, it should remain unchanged
                            self.assertEqual(result_spec, spec)

    def test_parse_constraints_string_spec_no_crash(self):
        """Specific test to ensure string spec doesn't cause AttributeError."""
        dialect = OceanBaseDialect()
        parser = OceanBaseTableDefinitionParser(dialect, dialect.preparer, default_schema="test")
        
        # Mock parent method to return string spec (the problematic case)
        with patch.object(parser.__class__.__bases__[0], '_parse_constraints', return_value=("fk_constraint", "string_spec")):
            # This should not raise AttributeError: 'str' object has no attribute 'get'
            try:
                result = parser._parse_constraints("dummy line")
                # If we get here, the bug is fixed
                self.assertIsNotNone(result)
                tp, spec = result
                self.assertEqual(tp, "fk_constraint")
                self.assertEqual(spec, "string_spec")
            except AttributeError as e:
                if "'str' object has no attribute 'get'" in str(e):
                    self.fail("The bug still exists: string spec caused AttributeError")
                else:
                    raise  # Re-raise if it's a different AttributeError
