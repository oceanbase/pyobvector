import unittest
from pyobvector import *
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ObVecJsonTableTest(unittest.TestCase):
    def setUp(self) -> None:
        self.common_client = ObVecClient()
        self.common_client.perform_raw_text_sql("TRUNCATE TABLE _data_json_t")
        self.common_client.perform_raw_text_sql("TRUNCATE TABLE _meta_json_t")
        self.client = ObVecJsonTableClient(user_id=1)
    
    def test_create_and_alter_jtable(self):
        self.client.perform_json_table_sql(
            "create table `t2` (c1 int NOT NULL DEFAULT 10, c2 varchar(30) DEFAULT 'ca', c3 varchar not null, c4 decimal(10, 2));"
        )
        tmp_client = ObVecJsonTableClient(user_id=1)
        self.assertEqual(tmp_client.jmetadata.meta_cache['t2'], 
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'c2', 'jcol_type': 'VARCHAR(30)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'ca'}, 
                {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'VARCHAR', 'jcol_nullable': False, 'jcol_has_default': False, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t2 CHANGE COLUMN c2 changed_col INT"
        )
        self.assertEqual(self.client.jmetadata.meta_cache['t2'],
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None}, 
                {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'VARCHAR', 'jcol_nullable': False, 'jcol_has_default': False, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t2 DROP c3"
        )
        self.assertEqual(self.client.jmetadata.meta_cache['t2'],
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t2 ADD COLUMN email VARCHAR(100) default 'example@example.com'"
        )
        self.assertEqual(self.client.jmetadata.meta_cache['t2'],
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 20, 'jcol_name': 'email', 'jcol_type': 'VARCHAR(100)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'example@example.com'}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t2 MODIFY COLUMN c4 INT NOT NULL DEFAULT 100"
        )
        self.assertEqual(self.client.jmetadata.meta_cache['t2'],
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '100'},
                {'jcol_id': 20, 'jcol_name': 'email', 'jcol_type': 'VARCHAR(100)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'example@example.com'}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t2 RENAME TO alter_test"
        )
        self.assertEqual(self.client.jmetadata.meta_cache.get('t2', []), [])
        self.assertEqual(self.client.jmetadata.meta_cache['alter_test'],
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '100'},
                {'jcol_id': 20, 'jcol_name': 'email', 'jcol_type': 'VARCHAR(100)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'example@example.com'}
            ]
        )
