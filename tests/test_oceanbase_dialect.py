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
