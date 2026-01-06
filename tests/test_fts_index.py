import unittest
from pyobvector import *
from sqlalchemy import Column, Integer, text
from sqlalchemy.dialects.mysql import TEXT
import logging

logger = logging.getLogger(__name__)

class ObFtsIndexTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ObVecClient()
    
    def test_fts_index(self):
        """Test basic full-text index creation"""
        test_collection_name = "fts_simple_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("doc", TEXT),
        ]
        self.client.create_table(
            test_collection_name,
            columns=cols,
        )
        fts_index_param = FtsIndexParam(
            index_name="fts_idx",
            field_names=["doc"],
            parser_type=FtsParser.NGRAM,
        )
        self.client.create_fts_idx_with_fts_index_param(
            test_collection_name,
            fts_idx_param=fts_index_param,
        )

        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("doc", TEXT),
        ]
        fts_index_param = FtsIndexParam(
            index_name="fts_idx",
            field_names=["doc"],
            parser_type=FtsParser.NGRAM,
        )
        self.client.create_table_with_index_params(
            table_name=test_collection_name,
            columns=cols,
            fts_idxs=[fts_index_param],
        )
        
    def test_fts_insert_and_search(self):
        """Test full-text index insert and search functionality"""
        test_collection_name = "fts_data_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", Integer, primary_key=True),
            Column("doc", TEXT),
        ]
        fts_index_param = FtsIndexParam(
            index_name="fts_idx",
            field_names=["doc"],
            parser_type=FtsParser.NGRAM,
        )
        self.client.create_table_with_index_params(
            table_name=test_collection_name,
            columns=cols,
            fts_idxs=[fts_index_param],
        )
        
        datas = [
            { "id": 1, "doc": "pLease porridge in the pot", },
            { "id": 2, "doc": "please say sorry", },
            { "id": 3, "doc": "nine years old", },
            { "id": 4, "doc": "some like it hot, some like it cold", },
            { "id": 5, "doc": "i like coding", },
            { "id": 6, "doc": "i like my company", },
        ]
        self.client.insert(
            test_collection_name,
            data = datas
        )

        res = self.client.get(
            test_collection_name,
            ids=None,
            where_clause=[MatchAgainst('like', 'doc'), text("id > 4")],
            output_column_name=["id", "doc"],
            n_limits=1,
        )
        self.assertEqual(
            set(res.fetchall()),
            set(
                [
                    (5, 'i like coding'),
                ]
            )
        )

    def test_fts_parser_types(self):
        """Test all supported full-text index parser types"""
        parser_types = [
            FtsParser.IK,
            FtsParser.NGRAM,
            FtsParser.NGRAM2,
            FtsParser.BASIC_ENGLISH,
        ]
        
        for parser_type in parser_types:
            test_collection_name = f"fts_test_{parser_type.name.lower()}"
            self.client.drop_table_if_exist(test_collection_name)
            
            cols = [
                Column("id", Integer, primary_key=True, autoincrement=False),
                Column("doc", TEXT),
            ]
            
            fts_index_param = FtsIndexParam(
                index_name=f"fts_idx_{parser_type.name.lower()}",
                field_names=["doc"],
                parser_type=parser_type,
            )
            
            # Test creating table and index
            try:
                self.client.create_table_with_index_params(
                    table_name=test_collection_name,
                    columns=cols,
                    fts_idxs=[fts_index_param],
                )
                
                # Insert test data
                test_data = [
                    {"id": 1, "doc": "OceanBase is a distributed database"},
                    {"id": 2, "doc": "全文索引测试 Full text search test"},
                    {"id": 3, "doc": "我喜欢编程 I like coding"},
                ]
                self.client.insert(test_collection_name, data=test_data)
                
                # Test search
                res = self.client.get(
                    test_collection_name,
                    ids=None,
                    where_clause=[MatchAgainst('test', 'doc')],
                    output_column_name=["id", "doc"],
                    n_limits=10,
                )
                rows = res.fetchall()
                # Verify that full-text search actually works by checking at least one result
                self.assertGreater(len(rows), 0, f"{parser_type.name} parser should return at least one result for 'test'")
                
            except (ImportError, NameError, AttributeError) as e:
                # JIEBA may require additional installation, skip only when Jieba is missing
                if "jieba" in str(e).lower():
                    logger.warning(f"Skipping {parser_type.name} parser test due to missing Jieba dependency: {e}")
                    continue
                raise
            finally:
                self.client.drop_table_if_exist(test_collection_name)

    def test_fts_default_parser(self):
        """Test default parser (None)"""
        test_collection_name = "fts_default_parser_test"
        self.client.drop_table_if_exist(test_collection_name)
        
        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("doc", TEXT),
        ]
        
        # Use default parser (None)
        fts_index_param = FtsIndexParam(
            index_name="fts_idx_default",
            field_names=["doc"],
            parser_type=None,  # Default Space parser
        )
        
        self.client.create_table_with_index_params(
            table_name=test_collection_name,
            columns=cols,
            fts_idxs=[fts_index_param],
        )
        
        test_data = [
            {"id": 1, "doc": "test default parser"},
            {"id": 2, "doc": "another test document"},
        ]
        self.client.insert(test_collection_name, data=test_data)
        
        # Test search
        res = self.client.get(
            test_collection_name,
            ids=None,
            where_clause=[MatchAgainst('test', 'doc')],
            output_column_name=["id", "doc"],
            n_limits=10,
        )
        rows = res.fetchall()
        # Verify that default parser actually works by checking at least one result
        self.assertGreater(len(rows), 0, "Default parser should return at least one result for 'test'")
        
        self.client.drop_table_if_exist(test_collection_name)

    def test_fts_multi_fields(self):
        """Test full-text index on multiple fields"""
        test_collection_name = "fts_multi_fields_test"
        self.client.drop_table_if_exist(test_collection_name)
        
        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("title", TEXT),
            Column("content", TEXT),
        ]
        
        # Create full-text indexes for multiple fields
        fts_indexes = [
            FtsIndexParam(
                index_name="fts_idx_title",
                field_names=["title"],
                parser_type=FtsParser.IK,
            ),
            FtsIndexParam(
                index_name="fts_idx_content",
                field_names=["content"],
                parser_type=FtsParser.NGRAM,
            ),
        ]
        
        self.client.create_table_with_index_params(
            table_name=test_collection_name,
            columns=cols,
            fts_idxs=fts_indexes,
        )
        
        test_data = [
            {"id": 1, "title": "OceanBase 数据库", "content": "OceanBase is a distributed database"},
            {"id": 2, "title": "全文索引", "content": "Full text search index"},
        ]
        self.client.insert(test_collection_name, data=test_data)
        
        # Test searching title field
        res_title = self.client.get(
            test_collection_name,
            ids=None,
            where_clause=[MatchAgainst('数据库', 'title')],
            output_column_name=["id", "title"],
            n_limits=10,
        )
        rows_title = res_title.fetchall()
        # Verify that title field search works
        self.assertGreater(len(rows_title), 0, "Title field search should return at least one result")
        
        # Test searching content field
        res_content = self.client.get(
            test_collection_name,
            ids=None,
            where_clause=[MatchAgainst('database', 'content')],
            output_column_name=["id", "content"],
            n_limits=10,
        )
        rows_content = res_content.fetchall()
        # Verify that content field search works
        self.assertGreater(len(rows_content), 0, "Content field search should return at least one result")
        
        self.client.drop_table_if_exist(test_collection_name)

    def test_fts_chinese_search(self):
        """Test Chinese full-text search"""
        test_collection_name = "fts_chinese_test"
        self.client.drop_table_if_exist(test_collection_name)
        
        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("doc", TEXT),
        ]
        
        # Use IK parser, suitable for Chinese
        fts_index_param = FtsIndexParam(
            index_name="fts_idx_chinese",
            field_names=["doc"],
            parser_type=FtsParser.IK,
        )
        
        self.client.create_table_with_index_params(
            table_name=test_collection_name,
            columns=cols,
            fts_idxs=[fts_index_param],
        )
        
        test_data = [
            {"id": 1, "doc": "海洋数据库 OceanBase"},
            {"id": 2, "doc": "全文索引功能测试"},
            {"id": 3, "doc": "我喜欢使用 OceanBase 数据库"},
            {"id": 4, "doc": "测试数据 test data"},
        ]
        self.client.insert(test_collection_name, data=test_data)
        
        # Test Chinese search
        res = self.client.get(
            test_collection_name,
            ids=None,
            where_clause=[MatchAgainst('数据库', 'doc')],
            output_column_name=["id", "doc"],
            n_limits=10,
        )
        rows = res.fetchall()
        # Verify that Chinese full-text search works by checking at least one result
        self.assertGreater(len(rows), 0, "Chinese search should return at least one result for '数据库'")
        
        self.client.drop_table_if_exist(test_collection_name)

    def test_fts_case_insensitive(self):
        """Test case-insensitive full-text search"""
        test_collection_name = "fts_case_test"
        self.client.drop_table_if_exist(test_collection_name)
        
        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("doc", TEXT),
        ]
        
        fts_index_param = FtsIndexParam(
            index_name="fts_idx_case",
            field_names=["doc"],
            parser_type=FtsParser.NGRAM,
        )
        
        self.client.create_table_with_index_params(
            table_name=test_collection_name,
            columns=cols,
            fts_idxs=[fts_index_param],
        )
        
        test_data = [
            {"id": 1, "doc": "pLease porridge in the pot"},
            {"id": 2, "doc": "Please say sorry"},
            {"id": 3, "doc": "PLEASE test"},
        ]
        self.client.insert(test_collection_name, data=test_data)
        
        # Test that lowercase search should find uppercase content
        res = self.client.get(
            test_collection_name,
            ids=None,
            where_clause=[MatchAgainst('please', 'doc')],
            output_column_name=["id", "doc"],
            n_limits=10,
        )
        rows = res.fetchall()
        # Full-text search is usually case-insensitive, verify it finds all three records
        self.assertEqual(len(rows), 3, "Case-insensitive search should find all three records containing 'please'")
        
        self.client.drop_table_if_exist(test_collection_name)

    def test_fts_complex_query(self):
        """Test complex full-text search queries (combined with other conditions)"""
        test_collection_name = "fts_complex_test"
        self.client.drop_table_if_exist(test_collection_name)
        
        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("doc", TEXT),
            Column("category", Integer),
        ]
        
        fts_index_param = FtsIndexParam(
            index_name="fts_idx_complex",
            field_names=["doc"],
            parser_type=FtsParser.NGRAM,
        )
        
        self.client.create_table_with_index_params(
            table_name=test_collection_name,
            columns=cols,
            fts_idxs=[fts_index_param],
        )
        
        test_data = [
            {"id": 1, "doc": "i like coding", "category": 1},
            {"id": 2, "doc": "i like my company", "category": 1},
            {"id": 3, "doc": "i like reading", "category": 2},
            {"id": 4, "doc": "i like coding too", "category": 2},
        ]
        self.client.insert(test_collection_name, data=test_data)
        
        # Test full-text search combined with other conditions
        res = self.client.get(
            test_collection_name,
            ids=None,
            where_clause=[
                MatchAgainst('like', 'doc'),
                text("category = 1")
            ],
            output_column_name=["id", "doc", "category"],
            n_limits=10,
        )
        rows = res.fetchall()
        # Should only return records with category=1 and containing 'like' (expected: 2 records)
        self.assertEqual(len(rows), 2, "Complex query should return exactly 2 records with category=1 and containing 'like'")
        for row in rows:
            self.assertEqual(row[2], 1)  # category should be 1
        
        self.client.drop_table_if_exist(test_collection_name)

    def test_fts_create_after_insert(self):
        """Test creating full-text index after inserting data"""
        test_collection_name = "fts_create_after_insert_test"
        self.client.drop_table_if_exist(test_collection_name)
        
        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("doc", TEXT),
        ]
        
        # Create table first
        self.client.create_table(
            test_collection_name,
            columns=cols,
        )
        
        # Insert data
        test_data = [
            {"id": 1, "doc": "test document one"},
            {"id": 2, "doc": "test document two"},
        ]
        self.client.insert(test_collection_name, data=test_data)
        
        # Then create full-text index
        fts_index_param = FtsIndexParam(
            index_name="fts_idx_after",
            field_names=["doc"],
            parser_type=FtsParser.NGRAM,
        )
        self.client.create_fts_idx_with_fts_index_param(
            test_collection_name,
            fts_idx_param=fts_index_param,
        )
        
        # Test search
        res = self.client.get(
            test_collection_name,
            ids=None,
            where_clause=[MatchAgainst('test', 'doc')],
            output_column_name=["id", "doc"],
            n_limits=10,
        )
        rows = res.fetchall()
        # Verify that index created after insertion works (expected: 2 records containing 'test')
        self.assertEqual(len(rows), 2, "Index created after insertion should return 2 records containing 'test'")
        
        self.client.drop_table_if_exist(test_collection_name)
        