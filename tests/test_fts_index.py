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
            where_clause=[MatchAgaint('like', 'doc'), text("id > 4")],
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
        