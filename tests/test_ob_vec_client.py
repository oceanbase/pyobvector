import unittest
from pyobvector import *
from sqlalchemy import Column, Integer, JSON, String
from sqlalchemy import func
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ObVecClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ObVecClient() # Set your link string.

    def test_ann_search(self):
        test_collection_name = "ob_ann_test"
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
            distance_func=func.l2_distance,
            with_dist=True,
            topk=5,
            output_column_names=["id"],
        )
        self.assertEqual(set(res.fetchall()), set([(112,0.0), (111,0.0), (10,0.0), (11,0.0), (12,0.0)]))

        res = self.client.ann_search(
            test_collection_name,
            vec_data=[0, 0, 0],
            vec_column_name="embedding",
            distance_func=func.l2_distance,
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
            {"id":"abc", "embedding":[0.748479, 0.276979, 0.555195]},
            {"id":"bcd", "embedding":[0.748479, 0.276979, 0.555195]},
            {"id":"cde", "embedding":[0, 0, 0]},
            {"id":"def", "embedding":[1, 2, 3]},
        ]
        self.client.insert(test_collection_name, data=data)

        self.client.delete(test_collection_name, ids=["bcd", "def"])
        res = self.client.get(test_collection_name, ids=["abc", "bcd", "cde", "def"], output_column_name=['id'])
        self.assertEqual(set(res.fetchall()), set([("abc",), ("cde",)]))

    def test_set_variable(self):
        self.client.set_ob_hnsw_ef_search(100)
        self.assertEqual(self.client.get_ob_hnsw_ef_search(), 100)

if __name__ == "__main__":
    unittest.main()
