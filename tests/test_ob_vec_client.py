import json
import unittest
from pyobvector import *
from sqlalchemy import Column, Integer, JSON, String, text
from sqlalchemy import func
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ObVecClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ObVecClient() # Set your link string.

    def test_ann_search(self):
        test_collection_name = "OB文档_ann_test"
        self.client.drop_table_if_exist(test_collection_name)

        # create partitioned table
        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )

        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("embedding", VECTOR(3)),
            Column("meta", JSON),
        ]
        self.client.create_table(
            test_collection_name, columns=cols, partitions=range_part
        )

        # create vector index
        self.client.create_index(
            test_collection_name,
            is_vec_index=True,
            index_name="vidx",
            column_names=["embedding"],
            vidx_params="distance=l2, type=hnsw, lib=vsag",
        )

        # insert data
        vector_value1 = [0.748479, 0.276979, 0.555195]
        vector_value2 = [0, 0, 0]
        data1 = [{"id": i, "embedding": vector_value1} for i in range(10)]
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(10, 13)])
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(111, 113)])
        self.client.insert(test_collection_name, data=data1)

        # perform ann search
        res = self.client.ann_search(
            test_collection_name,
            vec_data=[0, 0, 0],
            vec_column_name="embedding",
            distance_func=l2_distance,
            with_dist=True,
            topk=5,
            output_column_names=["id"],
        )
        self.assertEqual(set(res.fetchall()), set([(112,0.0), (111,0.0), (10,0.0), (11,0.0), (12,0.0)]))

        res = self.client.ann_search(
            test_collection_name,
            vec_data=[0, 0, 0],
            vec_column_name="embedding",
            distance_func=l2_distance,
            with_dist=True,
            topk=5,
            output_column_names=["id"],
            partition_names=["p0"],
        )
        self.assertEqual(set([r[0] for r in res.fetchall()]), set([12, 11, 10, 5, 7]))

    def test_delete_get(self):
        test_collection_name = "ob_delete_get_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", String(64), primary_key=True, autoincrement=False),
            Column("embedding", VECTOR(3)),
            Column("meta", JSON),
        ]
        self.client.create_table(
            test_collection_name, columns=cols
        )

        # create vector index
        self.client.create_index(
            test_collection_name,
            is_vec_index=True,
            index_name="vidx",
            column_names=["embedding"],
            vidx_params="distance=l2, type=hnsw, lib=vsag",
        )

        # insert data
        data = [
            {"id":"abc", "embedding":[0.748479, 0.276979, 0.555195], "meta": {"page": 1}},
            {"id":"bcd", "embedding":[0.748479, 0.276979, 0.555195], "meta": {"page": 2}},
            {"id":"cde", "embedding":[0, 0, 0], "meta": {"page": 3}},
            {"id":"def", "embedding":[1, 2, 3], "meta": {"page": 4}},
        ]
        self.client.insert(test_collection_name, data=data)

        self.client.delete(test_collection_name, ids=["bcd", "def"])
        res = self.client.get(
            test_collection_name,
            ids=["abc", "bcd", "cde", "def"],
            where_clause=[text("meta->'$.page' > 1")],
            output_column_name=['id']
        )
        self.assertEqual(set(res.fetchall()), set([('cde',)]))

    def test_set_variable(self):
        self.client.set_ob_hnsw_ef_search(100)
        self.assertEqual(self.client.get_ob_hnsw_ef_search(), 100)

    def test_create_index_dup(self):
        test_collection_name = "ob_create_index_dup_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", String(64), primary_key=True, autoincrement=False),
            Column("embedding", VECTOR(3)),
            Column("meta", JSON),
        ]
        self.client.create_table(
            test_collection_name, columns=cols
        )

        # create vector index
        self.client.create_index(
            test_collection_name,
            is_vec_index=True,
            index_name="vidx",
            column_names=["embedding"],
            vidx_params="distance=l2, type=hnsw, lib=vsag",
        )

        self.client.create_index(
            test_collection_name,
            is_vec_index=True,
            index_name="vidx",
            column_names=["embedding"],
            vidx_params="distance=l2, type=hnsw, lib=vsag",
        )

    def test_array_column(self):
        test_collection_name = "ob_array_column_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(64)),
            Column("arr_c", ARRAY(String(64))),
            Column("arr_nested_c", ARRAY(ARRAY(Integer))),
        ]
        self.client.create_table(
            test_collection_name, columns=cols
        )

        data = [
            {"name": "Alice", "arr_c": ["tag1", "tag2"], "arr_nested_c": [[1, 2, 3, 4, 5]]},
            {"name": "Bob", "arr_c": ["tag2", "tag3"], "arr_nested_c": json.dumps([[6, 7, 8]])},
            {"name": "Charlie", "arr_c": ["tag1"], "arr_nested_c": [[9]]},
        ]
        self.client.insert(test_collection_name, data=data)

        res = self.client.get(
            test_collection_name,
            ids=None,
            where_clause=[text(f"array_contains(arr_c, 'tag1')")],
            output_column_name=["name", "arr_c", "arr_nested_c"]
        )
        for row in res.fetchall():
            name = row[0]
            if name == "Alice":
                self.assertEqual(row[1], ["tag1", "tag2"])
                self.assertEqual(row[2], [[1, 2, 3, 4, 5]])
            elif name == "Charlie":
                self.assertEqual(row[1], ["tag1"])
                self.assertEqual(row[2], [[9]])
            else:
                self.fail("Unexpected row: {}".format(row))

if __name__ == "__main__":
    unittest.main()
