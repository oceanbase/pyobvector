import unittest
import datetime
from decimal import Decimal
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

def get_all_rows(res):
    rows = []
    for r in res:
        rows.append(r)
    return rows

class ObVecJsonTableTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root_client = ObVecJsonTableClient(user_id='0', admin_id='0')
        self.client = ObVecJsonTableClient(user_id='e5a69db04c5ea54adf324907d4b8f364', admin_id='0', user="jtuser@test")
        self.client2 = ObVecJsonTableClient(user_id='11b6dab4c97244fc801797d0e9814074', admin_id='0', user="jtuser@test")
    
    def test_create_and_alter_jtable(self):
        self.root_client._reset()
        self.client.refresh_metadata()
        keys_to_check = ['jcol_id', 'jcol_name', 'jcol_type', 'jcol_nullable', 'jcol_has_default', 'jcol_default']
        self.client.perform_json_table_sql(
            "create table `t2` (c1 int NOT NULL DEFAULT 10, c2 varchar(30) DEFAULT 'ca', c3 varchar not null, c4 decimal(10, 2));"
        )
        tmp_client = ObVecJsonTableClient(user_id='e5a69db04c5ea54adf324907d4b8f364', admin_id='0')
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
        self.root_client._reset()
        self.client.refresh_metadata()
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
    
    def test_dml(self):
        self.root_client._reset()
        self.client.refresh_metadata()
        keys_to_check = ['jcol_id', 'jcol_name', 'jcol_type', 'jcol_nullable', 'jcol_has_default', 'jcol_default']
        self.client.perform_json_table_sql(
            "create table `t1` (c1 int DEFAULT NULL, c2 varchar(30) DEFAULT 'ca', c3 varchar not null, c4 decimal(10, 2), c5 TIMESTAMP DEFAULT '2024-12-30T03:35:30');"
        )
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t1'], keys_to_check), 
            [
                {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 17, 'jcol_name': 'c2', 'jcol_type': 'VARCHAR(30)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': "'ca'"},
                {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'VARCHAR', 'jcol_nullable': False, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 19, 'jcol_name': 'c4', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 20, 'jcol_name': 'c5', 'jcol_type': 'TIMESTAMP', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': "'2024-12-30T03:35:30'"}
            ]
        )

        self.client.perform_json_table_sql(
            "insert into t1 (c2, c3) values ('hello', 'foo'), ('world', 'bar')"
        )
        self.client.perform_json_table_sql(
            "insert into t1 values (1+2, 'baz', 'oceanbase', 12.3+45.6, '2024-12-30T06:56:00')"
        )
        res = self.client.perform_json_table_sql(
            "select * from t1"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (None, 'hello', 'foo', None, datetime.datetime(2024, 12, 30, 3, 35, 30)),
                (None, 'world', 'bar', None, datetime.datetime(2024, 12, 30, 3, 35, 30)),
                (3, 'baz', 'oceanbase', Decimal('57.89'), datetime.datetime(2024, 12, 30, 6, 56)),
            ]
        )

        self.client.perform_json_table_sql(
            "update t1 set c1=10+10, c2='updated' where c3='oceanbase'"
        )
        res = self.client.perform_json_table_sql(
            "select * from t1"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (None, 'hello', 'foo', None, datetime.datetime(2024, 12, 30, 3, 35, 30)),
                (None, 'world', 'bar', None, datetime.datetime(2024, 12, 30, 3, 35, 30)),
                (20, 'updated', 'oceanbase', Decimal('57.89'), datetime.datetime(2024, 12, 30, 6, 56)),
            ]
        )

        self.client.perform_json_table_sql(
            "delete from t1 where c1 is NULL"
        )
        res = self.client.perform_json_table_sql(
            "select * from t1"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (20, 'updated', 'oceanbase', Decimal('57.89'), datetime.datetime(2024, 12, 30, 6, 56)),
            ]
        )

        self.client.perform_json_table_sql(
            "select c1, c2, t1.c3 from t1 where c1 > 21"
        )
        self.assertEqual(get_all_rows(res), [])

        self.client.perform_json_table_sql(
            "alter table t1 drop column c3"
        )
        res = self.client.perform_json_table_sql(
            "select * from t1"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (20, 'updated', Decimal('57.89'), datetime.datetime(2024, 12, 30, 6, 56)),
            ]
        )

        self.client.perform_json_table_sql(
            "alter table t1 add column new_col TIMESTAMP default '2024-12-30T02:44:17'"
        )
        res = self.client.perform_json_table_sql(
            "select * from t1"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (20, 'updated', Decimal('57.89'), 
                 datetime.datetime(2024, 12, 30, 6, 56),
                 datetime.datetime(2024, 12, 30, 2, 44, 17)),
            ]
        )

        self.client.perform_json_table_sql(
            "alter table t1 modify column c4 INT DEFAULT 10"
        )
        res = self.client.perform_json_table_sql(
            "select * from t1"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (20, 'updated', 58, 
                 datetime.datetime(2024, 12, 30, 6, 56),
                 datetime.datetime(2024, 12, 30, 2, 44, 17)),
            ]
        )

        self.client.perform_json_table_sql(
            "alter table t1 change column c1 change_col DECIMAL(10,2)"
        )
        res = self.client.perform_json_table_sql(
            "select * from t1"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (20.00, 'updated', 58, 
                 datetime.datetime(2024, 12, 30, 6, 56),
                 datetime.datetime(2024, 12, 30, 2, 44, 17)),
            ]
        )

        self.client.perform_json_table_sql(
            "insert into t1 (change_col, c2, c4) values (12, 'pyobvector is good', 50), (90, 'oceanbase is good', 20)"
        )
        res = self.client.perform_json_table_sql(
            "select sum(c4) as c4_sum from t1 where CHAR_LENGTH(c2) > 10"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (Decimal('70'),),
            ]
        )

        res = self.client.perform_json_table_sql(
            "select * from t1 where CHAR_LENGTH(c2) > 10 or c4 > '50' order by c4"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (Decimal('90.00'), 'oceanbase is good', 20, datetime.datetime(2024, 12, 30, 3, 35, 30), datetime.datetime(2024, 12, 30, 2, 44, 17)),
                (Decimal('12.00'), 'pyobvector is good', 50, datetime.datetime(2024, 12, 30, 3, 35, 30), datetime.datetime(2024, 12, 30, 2, 44, 17)),
                (Decimal('20.00'), 'updated', 58, datetime.datetime(2024, 12, 30, 6, 56), datetime.datetime(2024, 12, 30, 2, 44, 17))
            ]
        )

    def test_col_name_conflict(self):
        self.root_client._reset()
        self.client.refresh_metadata()
        keys_to_check = ['jcol_id', 'jcol_name', 'jcol_type', 'jcol_nullable', 'jcol_has_default', 'jcol_default']
        self.client.perform_json_table_sql(
            "create table `t1` (user_id int DEFAULT NULL, jtable_name varchar(30) DEFAULT 'jtable');"
        )
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['t1'], keys_to_check), 
            [
                {'jcol_id': 16, 'jcol_name': 'user_id', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': None},
                {'jcol_id': 17, 'jcol_name': 'jtable_name', 'jcol_type': 'VARCHAR(30)', 'jcol_nullable': True, 'jcol_has_default': True, 'jcol_default': "'jtable'"},
            ]
        )

        self.client.perform_json_table_sql(
            "insert into t1 values (1, 'alice'), (2, 'bob')"
        )
        res = self.client.perform_json_table_sql(
            "select * from t1"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (1, 'alice'),
                (2, 'bob'),
            ]
        )

        res = self.client.perform_json_table_sql(
            "select * from t1 where user_id > 1"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (2, 'bob'),
            ]
        )

        res = self.client.perform_json_table_sql(
            "update t1 set user_id=15 where jtable_name='alice'"
        )
        res = self.client.perform_json_table_sql(
            "select * from t1"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                (15, 'alice'),
                (2, 'bob'),
            ]
        )

    def test_timestamp_datatype(self):
        self.root_client._reset()
        self.client.refresh_metadata()
        self.client.perform_json_table_sql(
            "create table `t1` (c1 int DEFAULT NULL, c2 TIMESTAMP);"
        )

        self.client.perform_json_table_sql(
            "insert into t1 values (1, CURRENT_DATE - INTERVAL '1' MONTH);"
        )

        self.client.perform_json_table_sql(
            "select * from t1"
        )

    def test_online_cases(self):
        self.root_client._reset()
        self.client.refresh_metadata()
        keys_to_check = ['jcol_id', 'jcol_name', 'jcol_type', 'jcol_nullable', 'jcol_has_default', 'jcol_default']
        self.client.perform_json_table_sql(
            "CREATE TABLE `table_unit_test`(id INT, field0 VARCHAR(1024), field1 INT, field2 DECIMAL(10,2), field3 timestamp NOT NULL default CURRENT_TIMESTAMP, field4 varchar(1024));"
        )
        logger.info(sub_dict(self.client.jmetadata.meta_cache['table_unit_test'], keys_to_check))
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['table_unit_test'], keys_to_check), 
            [
                {'jcol_id': 16, 'jcol_name': 'id', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 17, 'jcol_name': 'field0', 'jcol_type': 'VARCHAR(1024)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 18, 'jcol_name': 'field1', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 19, 'jcol_name': 'field2', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
                {'jcol_id': 20, 'jcol_name': 'field3', 'jcol_type': 'TIMESTAMP', 'jcol_nullable': False, 'jcol_has_default': True, 'jcol_default': 'CURRENT_TIMESTAMP()'},
                {'jcol_id': 21, 'jcol_name': 'field4', 'jcol_type': 'VARCHAR(1024)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
            ]
        )
        self.client.perform_json_table_sql(
            "INSERT INTO `table_unit_test` (id, field0, field1, field2, field4) VALUES (1, '汽车保养', 1, 1000.00, '4S 店')"
        )
        self.client.perform_json_table_sql(
            "INSERT INTO `table_unit_test` (id, field0, field1, field2, field3, field4) VALUES (2, '奶茶', 2, 15.00, CURRENT_TIMESTAMP(), '商场'), (3, '书', 2, 40.00, NOW() - INTERVAL '1' DAY, '商场'), (4, '手机', 2, 6000.00, '2025-03-09', '商场')"
        )

        res = self.client.perform_json_table_sql(
            "SELECT field0 AS 消费内容, field1 AS 消费类型, field2 AS 消费金额, field4 AS 消费地点 FROM `table_unit_test` LIMIT 2"
        )
        self.assertEqual(
            get_all_rows(res),
            [
                ('汽车保养', 1, Decimal('1000.00'), '4S 店'),
                ('奶茶', 2, Decimal('15.00'), '商场')
            ]
        )

        res = self.client.perform_json_table_sql(
            "SELECT field2 FROM `table_unit_test` WHERE field0 like '%汽车%' AND field4 like '%店%' ORDER BY field2 DESC LIMIT 2"
        )
        self.assertEqual(
            get_all_rows(res),
            [(Decimal('1000.00'),)]
        )

        res = self.client.perform_json_table_sql(
            "SELECT field0 AS 消费内容, `table_unit_test`.field1 AS 消费类型, field2 AS 消费金额 FROM `table_unit_test` WHERE DATE(`table_unit_test`.field3)='2025-03-09' ORDER BY 消费金额 DESC LIMIT 2"
        )
        # logger.info(get_all_rows(res))
        self.assertEqual(
            get_all_rows(res),
            [('手机', 2, Decimal('6000.00'))]
        )

        res = self.client.perform_json_table_sql(
            "SELECT field0, field1, field2, field3 FROM `table_unit_test` WHERE field4 like '%商场%' AND field3 <= CAST('2025-03-10' AS DATE) LIMIT 20"
        )
        self.assertEqual(
            get_all_rows(res),
            [('手机', 2, Decimal('6000.00'), datetime.datetime(2025, 3, 9, 0, 0))]
        )

        self.client.perform_json_table_sql(
            "UPDATE `table_unit_test` SET field3 = '2025-03-14' WHERE field0 = '汽车保养'"
        )
        res = self.client.perform_json_table_sql(
            "SELECT field0, field1, field2, field3 FROM `table_unit_test` WHERE DATE(field3) = '2025-03-14' AND field0 = '汽车保养'"
        )
        self.assertEqual(
            get_all_rows(res),
            [('汽车保养', 1, Decimal('1000.00'), datetime.datetime(2025, 3, 14, 0, 0))]
        )

        self.client.perform_json_table_sql(
            "UPDATE `table_unit_test` SET field0 = CONCAT(field0, '代办') WHERE DATE(field3) = '2025-03-14'"
        )
        res = self.client.perform_json_table_sql(
            "SELECT field0, field1, field2, field3 FROM `table_unit_test` WHERE DATE(field3) = '2025-03-14'"
        )
        self.assertEqual(
            get_all_rows(res),
            [('汽车保养代办', 1, Decimal('1000.00'), datetime.datetime(2025, 3, 14, 0, 0))]
        )

    def test_user_group(self):
        self.root_client._reset()
        self.client.refresh_metadata()
        self.client2.refresh_metadata()

        # check schemas sync
        keys_to_check = ['jcol_id', 'jcol_name', 'jcol_type', 'jcol_nullable', 'jcol_has_default', 'jcol_default']
        self.root_client.perform_json_table_sql(
            "CREATE TABLE `table_shared` (c1 INT, c2 VARCHAR(1024), c3 DECIMAL(10,2))"
        )
        logger.info(sub_dict(self.root_client.jmetadata.meta_cache['table_shared'], keys_to_check))
        target_schema = [
            {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
            {'jcol_id': 17, 'jcol_name': 'c2', 'jcol_type': 'VARCHAR(1024)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
            {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
        ]
        self.assertEqual(sub_dict(self.root_client.jmetadata.meta_cache['table_shared'], keys_to_check), target_schema)

        self.client.refresh_metadata()
        logger.info(sub_dict(self.client.jmetadata.meta_cache['table_shared'], keys_to_check))
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['table_shared'], keys_to_check), target_schema)

        self.client2.refresh_metadata()
        logger.info(sub_dict(self.client2.jmetadata.meta_cache['table_shared'], keys_to_check))
        self.assertEqual(sub_dict(self.client2.jmetadata.meta_cache['table_shared'], keys_to_check), target_schema)

        # Do ddl in different clients
        self.client.perform_json_table_sql(
            "INSERT INTO `table_shared` (c1, c2, c3) VALUES (1, 'foo', 10.0), (2, 'bar', 20.0)"
        )
        self.client2.perform_json_table_sql(
            "INSERT INTO `table_shared` (c1, c2, c3) VALUES (3, 'oceanbase', 100.0)"
        )
        res = self.client.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 'foo', Decimal('10.00')), (2, 'bar', Decimal('20.00'))]
        )
        res = self.client2.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(3, 'oceanbase', Decimal('100.00'))]
        )

        # multi-users mode
        mu_client = ObVecJsonTableClient(user_id=None, admin_id='0', user="jtuser@test")
        res = mu_client.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 'foo', Decimal('10.00')), (2, 'bar', Decimal('20.00')), (3, 'oceanbase', Decimal('100.00'))]
        )

        # More examples --- common clients first.
        self.client.perform_json_table_sql(
            "UPDATE `table_shared` SET c3 = c3 * 10 WHERE c1 = 1"
        )
        res = self.client.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 'foo', Decimal('100.00')), (2, 'bar', Decimal('20.00'))]
        )
        res = self.client2.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(3, 'oceanbase', Decimal('100.00'))],
        )

        self.client2.perform_json_table_sql(
            "UPDATE `table_shared` SET c2 = UPPER(c2) WHERE c3 = 100.0"
        )
        res = self.client.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 'foo', Decimal('100.00')), (2, 'bar', Decimal('20.00'))]
        )
        res = self.client2.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(3, 'OCEANBASE', Decimal('100.00'))]
        )

        res = mu_client.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 'foo', Decimal('100.00')), (2, 'bar', Decimal('20.00')), (3, 'OCEANBASE', Decimal('100.00'))]
        )

        # More examples ---  multi-users client first.
        mu_client.perform_json_table_sql(
            "DELETE FROM `table_shared` WHERE c3 = 100.0"
        )
        res = mu_client.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(2, 'bar', Decimal('20.00'))]
        )
        res = self.client.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(2, 'bar', Decimal('20.00'))]
        )
        res = self.client2.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            []
        )

    def test_select_with_data_id(self):
        self.root_client._reset()
        self.client.refresh_metadata()

        keys_to_check = ['jcol_id', 'jcol_name', 'jcol_type', 'jcol_nullable', 'jcol_has_default', 'jcol_default']
        self.root_client.perform_json_table_sql(
            "CREATE TABLE `table_shared` (c1 INT, c2 VARCHAR(1024), c3 DECIMAL(10,2))"
        )
        logger.info(sub_dict(self.root_client.jmetadata.meta_cache['table_shared'], keys_to_check))
        target_schema = [
            {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
            {'jcol_id': 17, 'jcol_name': 'c2', 'jcol_type': 'VARCHAR(1024)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
            {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
        ]
        self.assertEqual(sub_dict(self.root_client.jmetadata.meta_cache['table_shared'], keys_to_check), target_schema)

        self.client.refresh_metadata()
        logger.info(sub_dict(self.client.jmetadata.meta_cache['table_shared'], keys_to_check))
        self.assertEqual(sub_dict(self.client.jmetadata.meta_cache['table_shared'], keys_to_check), target_schema)

        self.client.perform_json_table_sql(
            "INSERT INTO `table_shared` (c1, c2, c3) VALUES (1, 'foo', 10.0), (2, 'bar', 20.0)"
        )

        res = self.client.perform_json_table_sql(
            "SELECT * FROM `table_shared`"
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 'foo', Decimal('10.00')), (2, 'bar', Decimal('20.00'))]
        )

        res = self.client.perform_json_table_sql(
            "SELECT data_json_t.jdata_id, * FROM `table_shared`",
            # select_with_data_id=True
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 1, 'foo', Decimal('10.00')), (2, 2, 'bar', Decimal('20.00'))]
        )

        res = self.client.perform_json_table_sql(
            "SELECT * FROM `table_shared`",
            select_with_data_id=True
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 1, 'foo', Decimal('10.00')), (2, 2, 'bar', Decimal('20.00'))]
        )

    def test_insert_update_delete_with_row_count(self):
        self.root_client._reset()

        keys_to_check = ['jcol_id', 'jcol_name', 'jcol_type', 'jcol_nullable', 'jcol_has_default', 'jcol_default']
        self.root_client.perform_json_table_sql(
            "CREATE TABLE `test_dml_with_row_count` (c1 INT, c2 VARCHAR(1024), c3 DECIMAL(10,2))"
        )
        logger.info(sub_dict(self.root_client.jmetadata.meta_cache['test_dml_with_row_count'], keys_to_check))
        target_schema = [
            {'jcol_id': 16, 'jcol_name': 'c1', 'jcol_type': 'INT', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
            {'jcol_id': 17, 'jcol_name': 'c2', 'jcol_type': 'VARCHAR(1024)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
            {'jcol_id': 18, 'jcol_name': 'c3', 'jcol_type': 'DECIMAL(10,2)', 'jcol_nullable': True, 'jcol_has_default': False, 'jcol_default': None},
        ]

        client_without_user_id = ObVecJsonTableClient(
            user_id= None,
            admin_id='0',
            user="jtuser@test"
        )
        
        row_cnt = client_without_user_id.perform_json_table_sql(
            "INSERT INTO `test_dml_with_row_count` (c1, c2, c3) VALUES (1, 'foo', 10.0), (2, 'bar', 20.0)",
            opt_user_id="1",
        )
        self.assertEqual(row_cnt, 2)

        row_cnt = client_without_user_id.perform_json_table_sql(
            "INSERT INTO `test_dml_with_row_count` (c1, c2, c3) VALUES (3, 'oceanbase', 100.0)",
            opt_user_id="2",
        )
        self.assertEqual(row_cnt, 1)

        res = client_without_user_id.perform_json_table_sql(
            "SELECT * FROM `test_dml_with_row_count`",
            opt_user_id="1",
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 'foo', Decimal('10.00')), (2, 'bar', Decimal('20.00'))]
        )

        res = client_without_user_id.perform_json_table_sql(
            "SELECT * FROM `test_dml_with_row_count`",
            opt_user_id="2",
        )
        self.assertEqual(
            get_all_rows(res),
            [(3, 'oceanbase', Decimal('100.00'))]
        )

        res = client_without_user_id.perform_json_table_sql(
            "SELECT * FROM `test_dml_with_row_count`",
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 'foo', Decimal('10.00')), (2, 'bar', Decimal('20.00')), (3, 'oceanbase', Decimal('100.00'))]
        )

        row_cnt = client_without_user_id.perform_json_table_sql(
            "UPDATE `test_dml_with_row_count` SET c2=UPPER(c2) WHERE c1=1",
            opt_user_id="1",
        )
        self.assertEqual(row_cnt, 1)
        res = client_without_user_id.perform_json_table_sql(
            "SELECT * FROM `test_dml_with_row_count`",
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 'FOO', Decimal('10.00')), (2, 'bar', Decimal('20.00')), (3, 'oceanbase', Decimal('100.00'))]
        )

        row_cnt = client_without_user_id.perform_json_table_sql(
            "UPDATE `test_dml_with_row_count` SET c2=UPPER(c2)",
        )
        self.assertEqual(row_cnt, 3)
        res = client_without_user_id.perform_json_table_sql(
            "SELECT * FROM `test_dml_with_row_count`",
        )
        self.assertEqual(
            get_all_rows(res),
            [(1, 'FOO', Decimal('10.00')), (2, 'BAR', Decimal('20.00')), (3, 'OCEANBASE', Decimal('100.00'))]
        )

        row_cnt = client_without_user_id.perform_json_table_sql(
            "DELETE FROM `test_dml_with_row_count` WHERE c1=3",
            opt_user_id="2"
        )
        self.assertEqual(row_cnt, 1)
        res = client_without_user_id.perform_json_table_sql(
            "SELECT * FROM `test_dml_with_row_count`",
        )
        self.assertEqual(get_all_rows(res), [(1, 'FOO', Decimal('10.00')), (2, 'BAR', Decimal('20.00'))])
        row_cnt = client_without_user_id.perform_json_table_sql(
            "DELETE FROM `test_dml_with_row_count`",
        )
        self.assertEqual(row_cnt, 2)
        res = client_without_user_id.perform_json_table_sql(
            "SELECT * FROM `test_dml_with_row_count`",
        )
        self.assertEqual(get_all_rows(res), [])
