import unittest
from pyobvector import *
from sqlalchemy import Column, Integer, Table, func, text, Index, String
import numpy as np
import logging

logger = logging.getLogger(__name__)

class ObMilkClientTestSparseVector(unittest.TestCase):
    def setUp(self) -> None:
        self.client = MilvusLikeClient()
    
    def test_create_collection_with_sparse_vector(self):
        collection_name = "t1_spiv"
        self.client.drop_collection(collection_name)

        schema = self.client.create_schema()

        schema.add_field(field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field(field_name="sparse_vector", datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=65535)

        self.client.create_collection(
            collection_name=collection_name, schema=schema,
        )
    
    def test_create_sparse_vector_collection_with_index(self):
        collection_name = "t2_spiv"
        self.client.drop_collection(collection_name)

        schema = self.client.create_schema()

        schema.add_field(field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field(field_name="sparse_vector", datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=65535)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="sparse_vector",
            index_type="daat",
            index_name="spiv_on_t2",
            metric_type="inner_product",
        )

        self.client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=idx_params,
        )

    def test_insert_sparse_vector(self):
        collection_name = "insert_sparse_vec"
        self.client.drop_collection(collection_name)

        schema = self.client.create_schema()

        schema.add_field(field_name="pk", datatype=DataType.INT32, is_primary=True, auto_id=True)
        schema.add_field(field_name="sparse_vector", datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=65535)

        self.client.create_collection(
            collection_name=collection_name, schema=schema,
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
            collection_name=collection_name,
            data=data,
        )
    
    def test_similarity_search(self):
        collection_name = "sparse_vec_simlarity_search"
        self.client.drop_collection(collection_name)

        schema = self.client.create_schema()

        schema.add_field(field_name="pk", datatype=DataType.INT32, is_primary=True, auto_id=True)
        schema.add_field(field_name="sparse_vector", datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=65535)

        self.client.create_collection(
            collection_name=collection_name, schema=schema,
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
            collection_name=collection_name,
            data=data,
        )

        query_data = {1: 0.2, 50: 0.4, 1000: 0.7}
        res = self.client.search(
            collection_name=collection_name,
            data=query_data,
            anns_field="sparse_vector",
            limit=2,
            output_fields=["pk", "text", "sparse_vector"],
            search_params={"metric_type": "neg_ip"}
        )
        self.assertEqual(
            res,
            [
                {'pk': 3, 'text': 'Hello, world!', 'sparse_vector': '{1:0.7,50:0.7,1000:0.9}'},
                {'pk': 2, 'text': 'information retrieval focuses on finding relevant information in large datasets.', 'sparse_vector': '{10:0.1,200:0.7,1000:0.9}'}
            ]
        )