import unittest
from pyobvector import *
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def sub_dict(d_list, keys):
    new_metas = []
    for meta in d_list:
        tmp = {
            k: meta[k] for k in keys
        }
        new_metas.append(tmp)
    return new_metas

class ObVecJsonTableTest(unittest.TestCase):
    def setUp(self) -> None:
        self.common_client = ObVecClient()
        self.common_client.perform_raw_text_sql("TRUNCATE TABLE _data_json_t")
        self.common_client.perform_raw_text_sql("TRUNCATE TABLE _meta_json_t")
        self.client = ObVecJsonTableClient(user_id=1)
    
    def test_create_and_alter_jtable(self):
        self.client._reset()
        keys_to_check = ['jcol_id', 'jcol_name', 'jcol_type', 'jcol_nullable', 'jcol_has_default', 'jcol_default']
        self.client.perform_json_table_sql(
            "create table `t2` (c1 int NOT NULL DEFAULT 10, c2 varchar(30) DEFAULT 'ca', c3 varchar not null, c4 decimal(10, 2));"
        )
        tmp_client = ObVecJsonTableClient(user_id=1)
        self.assertEqual(sub_dict(tmp_client.jmetadata.meta_cache['t2'], keys_to_check), 
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'c2', 'jcol_type': 'VARCHAR(30)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': "'ca'"}, 
                {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'VARCHAR', 'jcol_nullable': False, 'jcol_has_default': False, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t2 CHANGE COLUMN c2 changed_col INT"
        )
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t2'], keys_to_check),
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
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t2'], keys_to_check),
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t2 ADD COLUMN email VARCHAR(100) default 'example@example.com'"
        )
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t2'], keys_to_check),
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 20, 'jcol_name': 'email', 'jcol_type': 'VARCHAR(100)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': "'example@example.com'"}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t2 MODIFY COLUMN c4 INT NOT NULL DEFAULT 100"
        )
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t2'], keys_to_check),
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '100'},
                {'jcol_id': 20, 'jcol_name': 'email', 'jcol_type': 'VARCHAR(100)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': "'example@example.com'"}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t2 RENAME TO alter_test"
        )
        self.assertEqual(self.client.jmetadata.meta_cache.get('t2', []), [])
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['alter_test'], keys_to_check),
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '10'}, 
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None}, 
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'INT', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': '100'},
                {'jcol_id': 20, 'jcol_name': 'email', 'jcol_type': 'VARCHAR(100)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': "'example@example.com'"}
            ]
        )

    def test_create_and_alter_jtable_evil(self):
        self.client._reset()
        keys_to_check = ['jcol_id', 'jcol_name', 'jcol_type', 'jcol_nullable', 'jcol_has_default', 'jcol_default']
        self.client.perform_json_table_sql(
            "create table `t1` (c1 int DEFAULT NULL, c2 varchar(30) DEFAULT 'ca', c3 varchar not null, c4 decimal(10, 2), c5 TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
        )
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t1'], keys_to_check), 
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 17, 'jcol_name': 'c2', 'jcol_type': 'VARCHAR(30)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': "'ca'"},
                {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'VARCHAR', 'jcol_nullable': False, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 20, 'jcol_name': 'c5', 'jcol_type': 'TIMESTAMP', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'CURRENT_TIMESTAMP()'}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t1 CHANGE COLUMN c2 changed_col DECIMAL"
        )
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t1'], keys_to_check),
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'DECIMAL', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'VARCHAR', 'jcol_nullable': False, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 20, 'jcol_name': 'c5', 'jcol_type': 'TIMESTAMP', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'CURRENT_TIMESTAMP()'}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t1 ADD COLUMN date timestamp default current_timestamp"
        )
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t1'], keys_to_check),
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'DECIMAL', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'VARCHAR', 'jcol_nullable': False, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 20, 'jcol_name': 'c5', 'jcol_type': 'TIMESTAMP', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'CURRENT_TIMESTAMP()'},
                {'jcol_id': 21, 'jcol_name': 'date', 'jcol_type': 'TIMESTAMP', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'CURRENT_TIMESTAMP()'}
            ]
        )

        self.client.perform_json_table_sql(
            "ALTER TABLE t1 MODIFY COLUMN c4 INT DEFAULT NULL"
        )
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t1'], keys_to_check),
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 17, 'jcol_name': 'changed_col', 'jcol_type': 'DECIMAL', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'VARCHAR', 'jcol_nullable': False, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 20, 'jcol_name': 'c5', 'jcol_type': 'TIMESTAMP', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'CURRENT_TIMESTAMP()'},
                {'jcol_id': 21, 'jcol_name': 'date', 'jcol_type': 'TIMESTAMP', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'CURRENT_TIMESTAMP()'}
            ]
        )
    
    def test_insert(self):
        self.client._reset()
        keys_to_check = ['jcol_id', 'jcol_name', 'jcol_type', 'jcol_nullable', 'jcol_has_default', 'jcol_default']
        self.client.perform_json_table_sql(
            "create table `t1` (c1 int DEFAULT NULL, c2 varchar(30) DEFAULT 'ca', c3 varchar not null, c4 decimal(10, 2), c5 TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
        )
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t1'], keys_to_check), 
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 17, 'jcol_name': 'c2', 'jcol_type': 'VARCHAR(30)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': "'ca'"},
                {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'VARCHAR', 'jcol_nullable': False, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 20, 'jcol_name': 'c5', 'jcol_type': 'TIMESTAMP', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': 'CURRENT_TIMESTAMP()'}
            ]
        )

        self.client.perform_json_table_sql(
            "insert into t1 (c2, c3) values ('hello', 'foo'), ('world', 'bar')"
        )
        self.client.perform_json_table_sql(
            "insert into t1 values (1+2, 'baz', 'oceanbase', 12.3+45.6, CURRENT_TIMESTAMP)"
        )

        self.client.perform_json_table_sql(
            "update t1 set c1=10+10, c2='updated' where c3='oceanbase'"
        )

        self.client.perform_json_table_sql(
            "delete from t1 where c1 is NULL"
        )

        self.client.perform_json_table_sql(
            "select c1, c2, t1.c3 from t1 where c1 > 21"
        )
        self.client.perform_json_table_sql(
            "alter table t1 drop column c3"
        )
        self.client.perform_json_table_sql(
            "alter table t1 add column new_col TIMESTAMP default CURRENT_TIMESTAMP"
        )
        self.client.perform_json_table_sql(
            "alter table t1 modify column c4 INT DEFAULT 10"
        )
        self.client.perform_json_table_sql(
            "alter table t1 change column c1 change_col DECIMAL(10,2)"
        )
