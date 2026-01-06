import unittest
from pyobvector import *
import logging

logger = logging.getLogger(__name__)

class ObMilkClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = MilvusLikeClient() # Set your link string.

    def test_create_collection_default(self):
        self.client.drop_collection("items1")
        self.client.drop_collection("items2")

        self.client.create_collection(
            collection_name="items1",
            dimension=3,
        )

        self.client.create_collection(
            collection_name="items2",
            dimension=3,
            id_type=DataType.VARCHAR,
            max_length=16,
        )
        self.assertEqual(self.client.get_collection_stats("items1"), {"row_count": 0})
        self.assertEqual(self.client.get_collection_stats("items2"), {"row_count": 0})

    def test_rename_collection(self):
        old_name = "items2"
        new_name = "new_items2"
        self.client.drop_collection(old_name)
        self.client.drop_collection(new_name)
        self.assertFalse(self.client.has_collection(old_name))
        self.assertFalse(self.client.has_collection(new_name))
        self.client.create_collection(
            collection_name=old_name,
            dimension=3,
            id_type=DataType.VARCHAR,
            max_length=16,
        )
        self.assertTrue(self.client.has_collection(old_name))
        self.assertFalse(self.client.has_collection(new_name))
        self.client.rename_collection(old_name, new_name)
        self.assertFalse(self.client.has_collection(old_name))
        self.assertTrue(self.client.has_collection(new_name))

    def test_create_collection_with_schema(self):
        self.client.drop_collection("medium_articles_2020")

        schema = self.client.create_schema()
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="title", datatype=DataType.VARCHAR, max_length=512)
        schema.add_field(
            field_name="title_vector", datatype=DataType.FLOAT_VECTOR, dim=768
        )
        schema.add_field(field_name="link", datatype=DataType.VARCHAR, max_length=512)
        schema.add_field(field_name="reading_time", datatype=DataType.INT64)
        schema.add_field(
            field_name="publication", datatype=DataType.VARCHAR, max_length=512
        )
        schema.add_field(field_name="claps", datatype=DataType.INT64)
        schema.add_field(field_name="responses", datatype=DataType.INT64)

        self.client.create_collection(
            collection_name="medium_articles_2020", schema=schema
        )
        self.assertEqual(
            self.client.get_collection_stats("medium_articles_2020"), {"row_count": 0}
        )

    def test_create_collection_with_schema_and_partition_expr(self):
        self.client.drop_collection("medium_articles_2020")

        schema = self.client.create_schema(
            partitions=ObHashPartition("id", part_count=3)
        )
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="title", datatype=DataType.VARCHAR, max_length=512)
        schema.add_field(
            field_name="title_vector", datatype=DataType.FLOAT_VECTOR, dim=768
        )
        schema.add_field(field_name="link", datatype=DataType.VARCHAR, max_length=512)
        schema.add_field(field_name="reading_time", datatype=DataType.INT64)
        schema.add_field(
            field_name="publication", datatype=DataType.VARCHAR, max_length=512
        )
        schema.add_field(field_name="claps", datatype=DataType.INT64)
        schema.add_field(field_name="responses", datatype=DataType.INT64)

        self.client.create_collection(
            collection_name="medium_articles_2020", schema=schema
        )
        self.assertEqual(
            self.client.get_collection_stats("medium_articles_2020"), {"row_count": 0}
        )

    def test_create_collection_with_index_params(self):
        test_collection_name = "medium_articles_2020_1"
        self.client.drop_collection(test_collection_name)

        schema = self.client.create_schema(
            partitions=ObHashPartition("id", part_count=3)
        )
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="title", datatype=DataType.VARCHAR, max_length=512)
        schema.add_field(
            field_name="title_vector", datatype=DataType.FLOAT_VECTOR, dim=768
        )
        schema.add_field(field_name="link", datatype=DataType.VARCHAR, max_length=512)
        schema.add_field(field_name="reading_time", datatype=DataType.INT64)
        schema.add_field(
            field_name="publication", datatype=DataType.VARCHAR, max_length=512
        )
        schema.add_field(field_name="claps", datatype=DataType.INT64)
        schema.add_field(field_name="responses", datatype=DataType.INT64)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="title_vector",
            index_type="HNSW",
            index_name="vidx_title_vector",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )

    def test_create_collection_with_post_create_index(self):
        test_collection_name = "medium_articles_2020_2"
        self.client.drop_collection(test_collection_name)

        schema = self.client.create_schema(
            partitions=ObHashPartition("id", part_count=3)
        )
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="title", datatype=DataType.VARCHAR, max_length=512)
        schema.add_field(
            field_name="title_vector", datatype=DataType.FLOAT_VECTOR, dim=768
        )
        schema.add_field(field_name="link", datatype=DataType.VARCHAR, max_length=512)
        schema.add_field(field_name="reading_time", datatype=DataType.INT64)
        schema.add_field(
            field_name="publication", datatype=DataType.VARCHAR, max_length=512
        )
        schema.add_field(field_name="claps", datatype=DataType.INT64)
        schema.add_field(field_name="responses", datatype=DataType.INT64)

        self.client.create_collection(
            collection_name=test_collection_name, schema=schema
        )

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="title_vector",
            index_type="HNSW",
            index_name="vidx_title_vector",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )
        self.client.create_index(
            collection_name=test_collection_name,
            index_params=idx_params,
        )

    def test_insert_data(self):
        test_collection_name = "insert_test"
        self.client.drop_collection(test_collection_name)

        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )
        schema = self.client.create_schema(partitions=range_part)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)
        schema.add_field(field_name="meta", datatype=DataType.JSON, nullable=True)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type="HNSW",
            index_name="vidx",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )

        data = [
            {"id": 12, "embedding": [1, 2, 3], "meta": {"doc": "oceanbase document 1"}},
            {
                "id": 90,
                "embedding": [0.13, 0.123, 1.213],
                "meta": {"doc": "oceanbase document 1"},
            },
        ]
        self.client.insert(
            collection_name=test_collection_name, data=data, partition_name="p0"
        )

        data = [
            {"id": 112, "embedding": [1, 2, 3]},
            {"id": 190, "embedding": [0.13, 0.123, 1.213]},
        ]
        self.client.insert(
            collection_name=test_collection_name, data=data, partition_name="p1"
        )

    def test_delete_data(self):
        test_collection_name = "delete_test"
        self.client.drop_collection(test_collection_name)

        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )
        schema = self.client.create_schema(partitions=range_part)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)
        schema.add_field(field_name="meta", datatype=DataType.JSON, nullable=True)
        schema.add_field(field_name="tag", datatype=DataType.INT64)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type="HNSW",
            index_name="vidx",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )

        data = [
            {"id": 12, "embedding": [1, 2, 3], "tag": 32},
            {"id": 90, "embedding": [0.13, 0.123, 1.213], "tag": 1},
        ]
        self.client.insert(
            collection_name=test_collection_name, data=data, partition_name="p0"
        )

        data = [
            {"id": 112, "embedding": [1, 2, 3], "tag": 19},
            {"id": 190, "embedding": [0.13, 0.123, 1.213], "tag": 22},
        ]
        self.client.insert(
            collection_name=test_collection_name, data=data, partition_name="p1"
        )

        # delete with 'ids' and 'parition_name'
        self.client.delete(
            collection_name=test_collection_name, ids=[12, 112], partition_name="p0"
        )
        # delete with where clause
        table = self.client.load_table(collection_name=test_collection_name)
        # For example: where tag % 2 = 0
        where_clause = [table.c["tag"] % 2 == 0]
        self.client.delete(collection_name=test_collection_name, flter=where_clause)

        where_clause = [table.c["tag"] == 19]
        self.client.delete(
            collection_name=test_collection_name, ids=[90, 112], flter=where_clause
        )

    def test_query_and_get(self):
        test_collection_name = "query_test"
        self.client.drop_collection(test_collection_name)

        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )
        schema = self.client.create_schema(partitions=range_part)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)
        schema.add_field(field_name="meta", datatype=DataType.JSON, nullable=True)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type="HNSW",
            index_name="vidx",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )

        data = [
            {"id": 12, "embedding": [1, 2, 3], "meta": {"doc": "oceanbase document 1"}},
            {
                "id": 90,
                "embedding": [0.13, 0.123, 1.213],
                "meta": {"doc": "oceanbase document 1"},
            },
            {"id": 112, "embedding": [1, 2, 3], "meta": None},
            {"id": 190, "embedding": [0.13, 0.123, 1.213], "meta": None},
        ]
        self.client.insert(collection_name=test_collection_name, data=data)

        res = self.client.query(
            collection_name=test_collection_name, output_fields=["id"]
        )
        self.assertEqual(set([r['id'] for r in res]), set([112, 190, 12, 90]))

        table = self.client.load_table(collection_name=test_collection_name)
        where_clause = [table.c["id"] < 100]
        res = self.client.query(
            collection_name=test_collection_name,
            output_fields=["id"],
            flter=where_clause,
        )
        self.assertEqual(set([r['id'] for r in res]), set([12, 90]))

        res = self.client.query(
            collection_name=test_collection_name, flter=where_clause
        )
        self.assertEqual([row["id"] for row in res], [12, 90])
        self.assertEqual(
            [row["meta"] for row in res],
            [{"doc": "oceanbase document 1"}, {"doc": "oceanbase document 1"}],
        )

        res = self.client.get(
            collection_name=test_collection_name,
            output_fields=["id", "meta"],
            ids=[80, 12, 112],
        )
        res = sorted(res, key=lambda x: x["id"])
        self.assertEqual(
            res,
            [
                {"id": 12, "meta": {"doc": "oceanbase document 1"}},
                {"id": 112, "meta": None},
            ],
        )

    def test_ann_search(self):
        test_collection_name = "ann_test"
        self.client.drop_collection(test_collection_name)

        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )
        schema = self.client.create_schema(partitions=range_part)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)
        schema.add_field(field_name="meta", datatype=DataType.JSON, nullable=True)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type=VecIndexType.HNSW,
            index_name="vidx",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )

        vector_value1 = [0.748479, 0.276979, 0.555195]
        vector_value2 = [0, 0, 0]
        data1 = [{"id": i, "embedding": vector_value1} for i in range(10)]
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(10, 13)])
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(111, 113)])
        self.client.insert(collection_name=test_collection_name, data=data1)

        res = self.client.search(
            collection_name=test_collection_name,
            data=[0, 0, 0],
            anns_field="embedding",
            limit=5,
            output_fields=["id"],
        )
        self.assertEqual(
            set([r['id'] for r in res]), set([12, 111, 11, 112, 10])
        )

        res = self.client.search(
            collection_name=test_collection_name,
            data=[0, 0, 0],
            anns_field="embedding",
            limit=5,
            output_fields=["id", "embedding", "meta"],
            partition_names=["p1"],
            with_dist=True,
        )
        self.assertEqual(set([111, 112]), set([r["id"] for r in res]))
    
    def test_ann_search_inner_product(self):
        test_collection_name = "ann_test_ip"
        self.client.drop_collection(test_collection_name)

        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )
        schema = self.client.create_schema(partitions=range_part)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)
        schema.add_field(field_name="meta", datatype=DataType.JSON, nullable=True)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type=VecIndexType.HNSW,
            index_name="vidx",
            metric_type="inner_product",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )

        vector_value1 = [1, 0, 0]
        vector_value2 = [0, 0, 1]
        data1 = [{"id": i, "embedding": vector_value1} for i in range(10)]
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(10, 13)])
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(111, 113)])
        self.client.insert(collection_name=test_collection_name, data=data1)

        res = self.client.search(
            collection_name=test_collection_name,
            data=[0, 0, 1],
            anns_field="embedding",
            limit=5,
            output_fields=["id"],
            search_params={"metric_type": "neg_ip"}
        )
        self.assertEqual(
            set([r['id'] for r in res]), set([12, 111, 11, 112, 10])
        )
    
    def test_ann_search_cosine(self):
        test_collection_name = "ann_test_cosine"
        self.client.drop_collection(test_collection_name)

        schema = self.client.create_schema()
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)
        schema.add_field(field_name="meta", datatype=DataType.JSON, nullable=True)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type=VecIndexType.HNSW,
            index_name="vidx",
            metric_type="cosine",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )

        vector_value1 = [1.2, 0.5, 0.7]
        vector_value2 = [0.34, 0.3, 1.64]
        data1 = [{"id": i, "embedding": vector_value1} for i in range(10)]
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(10, 13)])
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(111, 113)])
        self.client.insert(collection_name=test_collection_name, data=data1)

        self.client.search(
            collection_name=test_collection_name,
            data=[0, 0, 1],
            anns_field="embedding",
            limit=5,
            output_fields=["id"],
            search_params={"metric_type": "cosine"}
        )

    def test_upsert_data(self):
        test_collection_name = "upsert_test"
        self.client.drop_collection(test_collection_name)

        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )
        schema = self.client.create_schema(partitions=range_part)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)
        schema.add_field(field_name="meta", datatype=DataType.JSON)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type=VecIndexType.HNSW,
            index_name="vidx",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )

        data = [
            {"id": 112, "embedding": [1, 2, 3], "meta": {'doc':'hhh1'}},
            {"id": 190, "embedding": [0.13, 0.123, 1.213], "meta": {'doc':'hhh2'}},
        ]
        self.client.upsert(collection_name=test_collection_name, data=data)

        data = [
            {"id": 112, "embedding": [0, 0, 0], "meta": {'doc':'HHH1'}},
            {"id": 190, "embedding": [0, 0, 0], "meta": {'doc':'HHH2'}},
        ]
        self.client.upsert(collection_name=test_collection_name, data=data)

    def test_query_and_get_with_partition(self):
        test_collection_name = "query_test"
        self.client.drop_collection(test_collection_name)

        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )
        schema = self.client.create_schema(partitions=range_part)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)
        schema.add_field(field_name="meta", datatype=DataType.JSON, nullable=True)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type=VecIndexType.HNSW,
            index_name="vidx",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )

        data = [
            {"id": 12, "embedding": [1, 2, 3], "meta": {"doc": "oceanbase document 1"}},
            {
                "id": 90,
                "embedding": [0.13, 0.123, 1.213],
                "meta": {"doc": "oceanbase document 1"},
            },
            {"id": 112, "embedding": [1, 2, 3], "meta": None},
            {"id": 190, "embedding": [0.13, 0.123, 1.213], "meta": None},
        ]
        self.client.insert(collection_name=test_collection_name, data=data)

        table = self.client.load_table(collection_name=test_collection_name)
        where_clause = [table.c["id"] < 80]
        res = self.client.query(
            collection_name=test_collection_name,
            partition_names=["p0"],
            flter=where_clause,
        )
        self.assertEqual(res[0]["id"], 12)

        res = self.client.get(
            collection_name=test_collection_name,
            output_fields=["id", "meta"],
            ids=[80, 12, 112],
        )
        self.assertEqual(len(res), 2)
        self.assertEqual(set([r['id'] for r in res]), set([12, 112]))

        res = self.client.get(
            collection_name=test_collection_name,
            output_fields=["id", "meta"],
            ids=[80, 12, 112],
            partition_names=["p1"],
        )
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["id"], 112)

    def test_drop_index(self):
        test_collection_name = "drop_index_test"
        self.client.drop_collection(test_collection_name)

        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )
        schema = self.client.create_schema(partitions=range_part)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type=VecIndexType.HNSW,
            index_name="vidx",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )
        self.client.drop_index(collection_name=test_collection_name, index_name="vidx")

    def test_multi_part_collection(self):
        test_collection_name = "multi_part_test"
        self.client.drop_collection(test_collection_name)

        list_columns_part = ObListPartition(
            True,
            list_part_infos=[
                RangeListPartInfo("p0", [1, 3]),
                RangeListPartInfo("p1", [4, 6]),
                RangeListPartInfo("p2", [7, 9]),
            ],
            col_name_list=["col1"],
        )
        range_columns_sub_part = ObSubRangePartition(
            True,
            range_part_infos=[
                RangeListPartInfo("mp0", 100),
                RangeListPartInfo("mp1", 200),
                RangeListPartInfo("mp2", 300),
            ],
            col_name_list=["col2"],
        )
        list_columns_part.add_subpartition(range_columns_sub_part)
        schema = self.client.create_schema(partitions=list_columns_part)
        schema.add_field(field_name="col1", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="col2", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type=VecIndexType.HNSW,
            index_name="vidx",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )

    def test_create_collection_with_collection_and_field_schema(self):
        test_collection_name = "ann_test2"
        self.client.drop_collection(test_collection_name)

        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )
        schema = CollectionSchema(
            fields = [
                FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
                FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=3),
                FieldSchema(name="meta", dtype=DataType.JSON)
            ],
            partitions=range_part,
        )

        idx_params = self.client.prepare_index_params()
        idx_params.add_index(
            field_name="embedding",
            index_type=VecIndexType.HNSW,
            index_name="vidx",
            metric_type="L2",
            params={"M": 16, "efConstruction": 256},
        )

        self.client.create_collection(
            collection_name=test_collection_name,
            schema=schema,
            index_params=idx_params,
        )
        

if __name__ == "__main__":
    unittest.main()
