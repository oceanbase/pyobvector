import unittest
from pyobvector import *
import logging

from sqlglot import parse_one

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class OceanBaseDialectTest(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()
    
    def test_drop_column(self):
        sql = "ALTER TABLE users DROP COLUMN age"
        ob_ast = parse_one(sql, dialect="oceanbase")
        logger.info(f"\n{repr(ob_ast)}")

        sql = "ALTER TABLE users DROP age"
        ob_ast = parse_one(sql, dialect="oceanbase")
        logger.info(f"\n{repr(ob_ast)}")

    def test_modify_column(self):
        sql = "ALTER TABLE users MODIFY COLUMN email VARCHAR(100) NOT NULL"
        ob_ast = parse_one(sql, dialect="oceanbase")
        logger.info(f"\n{repr(ob_ast)}")

        sql = "ALTER TABLE users MODIFY email VARCHAR(100) NOT NULL"
        ob_ast = parse_one(sql, dialect="oceanbase")
        logger.info(f"\n{repr(ob_ast)}")

        sql = "ALTER TABLE users MODIFY COLUMN email VARCHAR(100) NOT NULL DEFAULT 'ca'"
        ob_ast = parse_one(sql, dialect="oceanbase")
        logger.info(f"\n{repr(ob_ast)}")

    def test_change_column(self):
        sql = "ALTER TABLE users CHANGE COLUMN username user_name VARCHAR(50)"
        ob_ast = parse_one(sql, dialect="oceanbase")
        logger.info(f"\n{repr(ob_ast)}")

        sql = "ALTER TABLE users CHANGE username user_name VARCHAR(50)"
        ob_ast = parse_one(sql, dialect="oceanbase")
        logger.info(f"\n{repr(ob_ast)}")

    def test_isolation_level_with_none_server_version_info(self):
        """Test that get_isolation_level handles None server_version_info correctly."""
        from unittest.mock import Mock, MagicMock
        
        dialect = OceanBaseDialect()
        dialect.server_version_info = None
        
        # Mock connection with cursor
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ("READ-COMMITTED",)
        
        mock_dbapi_connection = Mock()
        mock_dbapi_connection.cursor.return_value = mock_cursor
        
        # Should not raise TypeError about None >= tuple
        dialect.get_isolation_level(mock_dbapi_connection)
        self.assertIsNotNone(dialect.server_version_info)

    def test_create_engine_with_isolation_level(self):
        """Test that create_engine works with isolation_level parameter."""
        from sqlalchemy import create_engine
        from sqlalchemy.dialects import registry
        
        registry.register("mysql.oceanbase", "pyobvector.schema.dialect", "OceanBaseDialect")
        
        connection_str = "mysql+oceanbase://root@test:@127.0.0.1:2881/test?charset=utf8mb4"
        
        try:
            engine = create_engine(connection_str, isolation_level="READ COMMITTED", connect_args={"connect_timeout": 1})
            self.assertIsNotNone(engine)
        except Exception as e:
            error_msg = str(e)
            if "Can't connect" in error_msg or "Connection refused" in error_msg:
                self.skipTest(f"Database not available: {e}")
            elif isinstance(e, TypeError) and "'>=' not supported between instances of 'NoneType'" in error_msg:
                self.fail(f"create_engine raised TypeError with isolation_level: {e}")
            raise
