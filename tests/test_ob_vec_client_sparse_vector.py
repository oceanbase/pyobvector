import json
import unittest
from pyobvector import *
from sqlalchemy import Column, Integer, JSON, String, text
from sqlalchemy import func
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ObVecClientTestSparseVector(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ObVecClient()
    
    def test_create_collection_with_sparse_vector(self):
        table_name = "obvec_t1_spiv"
        self.client.drop_table_if_exist(table_name)

        cols = [
            Column("pk", String(100), primary_key=True, autoincrement=False),
            Column("sparse_vector", SPARSE_VECTOR),
            Column("text", String(65535)),
        ]

        self.client.create_table(
            table_name=table_name,
            columns=cols,
        )
    
    def test_create_sparse_vector_collection_with_index(self):
        table_name = "obvec_t2_spiv"
        self.client.drop_table_if_exist(table_name)

        cols = [
            Column("pk", String(100), primary_key=True, autoincrement=False),
            Column("sparse_vector", SPARSE_VECTOR),
            Column("text", String(65535)),
        ]

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="sparse_vector",
            index_type="daat",
            index_name="spiv_on_t2",
            metric_type="inner_product",
        )
        self.client.create_table_with_index_params(
            table_name=table_name,
            columns=cols,
            vidxs=idx_params,
        )

    def test_insert_sparse_vector(self):
        table_name = "obvec_insert_sparse_vec"
        self.client.drop_table_if_exist(table_name)

        cols = [
            Column("pk", Integer, primary_key=True, autoincrement=True),
            Column("sparse_vector", SPARSE_VECTOR),
            Column("text", String(65535)),
        ]

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="sparse_vector",
            index_type="daat",
            index_name="spiv_on_t2",
            metric_type="inner_product",
        )
        self.client.create_table_with_index_params(
            table_name=table_name,
            columns=cols,
            vidxs=idx_params,
        )

        data = [
            {
                "text": "information retrieval is a field of study.",
                "sparse_vector": {1: 0.5, 100: 0.3, 500: 0.8}
            },
            {
                "text": "information retrieval focuses on finding relevant information in large datasets.",
                "sparse_vector": {10: 0.1, 200: 0.7, 1000: 0.9}
            }
        ]
        self.client.insert(
            table_name=table_name,
            data=data,
        )
    
    def test_similarity_search(self):
        table_name = "obvec_sparse_vec_simlarity_search"
        self.client.drop_table_if_exist(table_name)

        cols = [
            Column("pk", Integer, primary_key=True, autoincrement=True),
            Column("sparse_vector", SPARSE_VECTOR),
            Column("text", String(65535)),
        ]

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="sparse_vector",
            index_type="daat",
            index_name="spiv_on_t2",
            metric_type="inner_product",
        )
        self.client.create_table_with_index_params(
            table_name=table_name,
            columns=cols,
            vidxs=idx_params,
        )

        data = [
            {
                "text": "information retrieval is a field of study.",
                "sparse_vector": {1: 0.5, 100: 0.3, 500: 0.8}
            },
            {
                "text": "information retrieval focuses on finding relevant information in large datasets.",
                "sparse_vector": {10: 0.1, 200: 0.7, 1000: 0.9}
            },
            {
                "text": "Hello, world!",
                "sparse_vector": {1: 0.7, 50: 0.7, 1000: 0.9}
            }
        ]
        self.client.insert(
            table_name=table_name,
            data=data,
        )

        query_data = {1: 0.2, 50: 0.4, 1000: 0.7}
        res = self.client.ann_search(
            table_name=table_name,
            vec_data=query_data,
            vec_column_name="sparse_vector",
            distance_func=negative_inner_product,
            with_dist=False,
            topk=2,
            output_column_names=["pk", "text", "sparse_vector"],
        )
        self.assertEqual(
            res.fetchall(),
            [
                (3, 'Hello, world!', '{1:0.7,50:0.7,1000:0.9}'),
                (2, 'information retrieval focuses on finding relevant information in large datasets.', '{10:0.1,200:0.7,1000:0.9}')
            ]
        )