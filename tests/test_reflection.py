import unittest
from pyobvector import *
import logging
from unittest.mock import Mock, patch
from pyobvector.schema.reflection import OceanBaseTableDefinitionParser
from sqlalchemy.dialects.mysql.reflection import MySQLTableDefinitionParser
import copy  # Added for deepcopy

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
        from pyobvector.schema.reflection import OceanBaseTableDefinitionParser
        
        # Create a mock parser class to test our specific method
        class MockParser(OceanBaseTableDefinitionParser):
            def __init__(self):
                # Skip the parent __init__ to avoid _prep_regexes issues
                self.default_schema = "test"
        
        parser = MockParser()
        
        # Test cases: we'll mock the parent method to return what we want to test
        test_cases = [
            {
                "name": "String spec (the bug case)",
                "parent_return": ("fk_constraint", "some_string_spec"),
                "expected_result": ("fk_constraint", "some_string_spec")  # Should remain unchanged
            },
            {
                "name": "Dict spec with restrict values",
                "parent_return": ("fk_constraint", {
                    "table": ["test", "other_table"],
                    "onupdate": "restrict", 
                    "ondelete": "restrict"
                }),
                "expected_result": ("fk_constraint", {
                    "table": ["other_table"],  # Should be trimmed
                    "onupdate": None,  # Should be None
                    "ondelete": None   # Should be None
                })
            },
            {
                "name": "Dict spec with cascade values", 
                "parent_return": ("fk_constraint", {
                    "table": ["other_table"],
                    "onupdate": "cascade",
                    "ondelete": "cascade"
                }),
                "expected_result": ("fk_constraint", {
                    "table": ["other_table"],
                    "onupdate": "cascade",  # Should remain unchanged
                    "ondelete": "cascade"   # Should remain unchanged
                })
            },
            {
                "name": "Dict spec with None values",
                "parent_return": ("fk_constraint", {
                    "table": ["other_table"],
                    "onupdate": None,
                    "ondelete": None
                }),
                "expected_result": ("fk_constraint", {
                    "table": ["other_table"],
                    "onupdate": None,  # Should remain None
                    "ondelete": None   # Should remain None
                })
            },
            {
                "name": "Dict spec without table key",
                "parent_return": ("fk_constraint", {
                    "onupdate": "restrict",
                    "ondelete": "cascade"
                }),
                "expected_result": ("fk_constraint", {
                    "onupdate": None,  # Should be None
                    "ondelete": "cascade"   # Should remain unchanged
                })
            },
            {
                "name": "Dict spec with single table (no trimming)",
                "parent_return": ("fk_constraint", {
                    "table": ["other_table"],
                    "onupdate": "restrict"
                }),
                "expected_result": ("fk_constraint", {
                    "table": ["other_table"],  # Should remain unchanged (only 1 element)
                    "onupdate": None  # Should be None
                })
            },
            {
                "name": "Dict spec with empty dict",
                "parent_return": ("fk_constraint", {}),
                "expected_result": ("fk_constraint", {})  # Should remain unchanged
            },
            {
                "name": "Dict spec with None table",
                "parent_return": ("fk_constraint", {
                    "table": None,
                    "onupdate": "restrict"
                }),
                "expected_result": ("fk_constraint", {
                    "table": None,  # Should remain unchanged (not a list)
                    "onupdate": None  # Should be None
                })
            },
            {
                "name": "Dict spec with non-list table",
                "parent_return": ("fk_constraint", {
                    "table": "not_a_list",
                    "ondelete": "restrict"
                }),
                "expected_result": ("fk_constraint", {
                    "table": "not_a_list",  # Should remain unchanged (not a list)
                    "ondelete": None  # Should be None
                })
            },
            {
                "name": "None spec",
                "parent_return": ("fk_constraint", None),
                "expected_result": ("fk_constraint", None)  # Should remain unchanged
            },
            {
                "name": "Non-fk constraint with string spec",
                "parent_return": ("unique", "string_spec"),
                "expected_result": ("unique", "string_spec")  # Should remain unchanged
            }
        ]
        
        for test_case in test_cases:
            with self.subTest(name=test_case["name"]):
                # Create a copy of the input to avoid mutation issues
                import copy
                parent_return = copy.deepcopy(test_case["parent_return"])
                expected_result = test_case["expected_result"]
                
                # Mock the parent class method to return our test input
                with patch.object(MySQLTableDefinitionParser, '_parse_constraints', return_value=parent_return):
                    # Call our method - this should apply our bugfix logic
                    result = parser._parse_constraints("dummy line")
                    
                    # Verify the result
                    self.assertIsNotNone(result)
                    result_tp, result_spec = result
                    expected_tp, expected_spec = expected_result
                    
                    self.assertEqual(result_tp, expected_tp)
                    
                    # For detailed comparison
                    if isinstance(expected_spec, dict) and isinstance(result_spec, dict):
                        for key, expected_value in expected_spec.items():
                            actual_value = result_spec.get(key)
                            self.assertEqual(actual_value, expected_value, 
                                f"Test '{test_case['name']}': Key '{key}' expected {expected_value}, got {actual_value}")
                    else:
                        self.assertEqual(result_spec, expected_spec,
                            f"Test '{test_case['name']}': Expected {expected_spec}, got {result_spec}")

    def test_parse_constraints_string_spec_no_crash(self):
        """Specific test to ensure string spec doesn't cause AttributeError."""
        from pyobvector.schema.reflection import OceanBaseTableDefinitionParser
        
        # Create a mock parser class to test our specific method
        class MockParser(OceanBaseTableDefinitionParser):
            def __init__(self):
                # Skip the parent __init__ to avoid _prep_regexes issues
                self.default_schema = "test"
        
        parser = MockParser()
        
        # Mock parent method to return string spec (the problematic case)
        with patch.object(MySQLTableDefinitionParser, '_parse_constraints', return_value=("fk_constraint", "string_spec")):
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
