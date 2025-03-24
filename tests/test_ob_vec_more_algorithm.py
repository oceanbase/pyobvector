import unittest
from pyobvector import *
from sqlalchemy import Column, Integer, JSON, text
from sqlalchemy.dialects.mysql import TEXT
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ObVecMoreAlgorithmTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ObVecClient()

    def test_hnswsq(self):
        test_collection_name = "hnswsq_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("embedding", VECTOR(3)),
            Column("meta", JSON),
        ]
        self.client.create_table(
            test_collection_name, columns=cols
        )

        vidx_param = IndexParam(
            index_name="vidx",
            field_name="embedding",
            index_type=VecIndexType.HNSW_SQ,
        )
        self.client.create_vidx_with_vec_index_param(
            test_collection_name,
            vidx_param,
        )

        vector_value1 = [0.748479, 0.276979, 0.555195]
        vector_value2 = [0, 0, 0]
        data1 = [{"id": i, "embedding": vector_value1} for i in range(10)]
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(10, 13)])
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(111, 113)])
        self.client.insert(test_collection_name, data=data1)

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

    def test_ivfflat(self):
        test_collection_name = "ivf_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("embedding", VECTOR(3)),
            Column("meta", JSON),
        ]
        self.client.create_table(
            test_collection_name, columns=cols
        )

        vector_value1 = [0.748479, 0.276979, 0.555195]
        vector_value2 = [0, 0, 0]
        data1 = [{"id": i, "embedding": vector_value1} for i in range(10)]
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(10, 13)])
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(111, 113)])
        self.client.insert(test_collection_name, data=data1)

        vidx_param = IndexParam(
            index_name="vidx",
            field_name="embedding",
            index_type=VecIndexType.IVFFLAT,
        )
        self.client.create_vidx_with_vec_index_param(
            test_collection_name,
            vidx_param,
        )

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

    def test_ivfsq(self):
        test_collection_name = "ivfsq_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("embedding", VECTOR(3)),
            Column("meta", JSON),
        ]
        self.client.create_table(
            test_collection_name, columns=cols
        )

        vector_value1 = [0.748479, 0.276979, 0.555195]
        vector_value2 = [0, 0, 0]
        data1 = [{"id": i, "embedding": vector_value1} for i in range(10)]
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(10, 13)])
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(111, 113)])
        self.client.insert(test_collection_name, data=data1)

        vidx_param = IndexParam(
            index_name="vidx",
            field_name="embedding",
            index_type=VecIndexType.IVFSQ,
        )
        self.client.create_vidx_with_vec_index_param(
            test_collection_name,
            vidx_param,
        )

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

    def test_ivfpq(self):
        test_collection_name = "ivfpq_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("embedding", VECTOR(4)),
            Column("meta", JSON),
        ]
        self.client.create_table(
            test_collection_name, columns=cols
        )

        vector_value1 = [0.748479, 0.276979, 0.555195, 0.13234]
        vector_value2 = [0, 0, 0, 0]
        data1 = [{"id": i, "embedding": vector_value1} for i in range(10)]
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(10, 13)])
        data1.extend([{"id": i, "embedding": vector_value2} for i in range(111, 113)])
        self.client.insert(test_collection_name, data=data1)

        vidx_param = IndexParam(
            index_name="vidx",
            field_name="embedding",
            index_type=VecIndexType.IVFPQ,
            params={
                "m": 2,
            }
        )
        self.client.create_vidx_with_vec_index_param(
            test_collection_name,
            vidx_param,
        )

        res = self.client.ann_search(
            test_collection_name,
            vec_data=[0, 0, 0, 0],
            vec_column_name="embedding",
            distance_func=l2_distance,
            with_dist=True,
            topk=5,
            output_column_names=["id"],
        )
        self.assertEqual(set(res.fetchall()), set([(112,0.0), (111,0.0), (10,0.0), (11,0.0), (12,0.0)]))

    def test_vec_fts_hybrid(self):
        test_collection_name = "vec_fts_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("embedding", VECTOR(3)),
            Column("doc", TEXT),
        ]
        self.client.create_table(
            test_collection_name, columns=cols
        )

        vidx_param = IndexParam(
            index_name="vidx",
            field_name="embedding",
            index_type=VecIndexType.HNSW,
        )
        self.client.create_vidx_with_vec_index_param(
            test_collection_name,
            vidx_param,
        )
        fts_param = FtsIndexParam(
            index_name="fts_idx",
            field_names=["doc"],
            parser_type=None,
        )
        self.client.create_fts_idx_with_fts_index_param(
            test_collection_name,
            fts_param
        )

        datas = [
            { "id": 1, "embedding":[1,2,3], "doc": "pLease porridge in the pot", },
            { "id": 2, "embedding":[0,0,0], "doc": "please say sorry", },
            { "id": 3, "embedding":[1,1,1], "doc": "nine years old", },
            { "id": 4, "embedding":[0,1,0], "doc": "some like it hot, some like it cold", },
            { "id": 5, "embedding":[100,100,100], "doc": "i like coding", },
            { "id": 6, "embedding":[0,0,0], "doc": "i like my company", },
        ]
        self.client.insert(
            test_collection_name,
            data=datas
        )

        # res = self.client.ann_search(
        #     test_collection_name,
        #     vec_data=[0, 0, 0],
        #     vec_column_name="embedding",
        #     distance_func=l2_distance,
        #     with_dist=True,
        #     topk=5,
        #     output_column_names=["id", "doc"],
        #     where_clause=[MatchAgainst('like', 'doc')]
        # )
        # for r in res.fetchall():
        #     logger.info(f"{r[0]} {r[1]}")

    def test_pre_post_filtering(self):
        test_collection_name = "pre_post_filtering_test"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("c1", Integer, primary_key=True, autoincrement=False),
            Column("c2", Integer),
            Column("c3", Integer),
            Column("v", VECTOR(3)),
            Column("doc", TEXT),
        ]
        self.client.create_table(
            test_collection_name, columns=cols
        )
        vidx_param = IndexParam(
            index_name="idx3",
            field_name="v",
            index_type=VecIndexType.HNSW,
        )
        self.client.create_vidx_with_vec_index_param(
            test_collection_name,
            vidx_param,
        )
        self.client.create_index(
            test_collection_name,
            is_vec_index=False,
            index_name="idx1",
            column_names=["c2"],
        )
        self.client.create_index(
            test_collection_name,
            is_vec_index=False,
            index_name="idx2",
            column_names=["c3"],
        )

        datas = [
            { "c1": 1, "c2": 1, "c3": 10, "v": [0.203846,0.205289,0.880265] },
            { "c1": 2, "c2": 2, "c3": 9, "v": [0.226980,0.579658,0.933939] },
            { "c1": 3, "c2": 3, "c3": 8, "v": [0.181664,0.013905,0.628127] },
            { "c1": 4, "c2": 4, "c3": 7, "v": [0.442633,0.637534,0.633993] },
            { "c1": 5, "c2": 5, "c3": 6, "v": [0.190118,0.959676,0.796483] },
            { "c1": 6, "c2": 6, "c3": 5, "v": [0.710370,0.007130,0.710913] },
            { "c1": 7, "c2": 7, "c3": 4, "v": [0.238120,0.289662,0.970101] },
            { "c1": 8, "c2": 8, "c3": 3, "v": [0.168794,0.567442,0.062338] },
            { "c1": 9, "c2": 9, "c3": 2, "v": [0.901419,0.676738,0.122339] },
            { "c1": 10, "c2": 10, "c3": 1, "v": [0.563644,0.811224,0.175574] },
        ]
        self.client.insert(
            test_collection_name,
            data=datas
        )

        res = self.client.ann_search(
            test_collection_name,
            vec_data=[0.712338,0.603321,0.133444],
            vec_column_name="v",
            distance_func=l2_distance,
            topk=5,
            output_column_names=["c1", "c2", "c3"],
            where_clause=[text("c2 > 5 and c3 < 6")],
            idx_name_hint="idx3"
        )
        self.assertEqual(
            set(res.fetchall()),
            set(
                [
                    (9, 9, 2),
                    (10, 10, 1),
                    (8, 8, 3),
                    (6, 6, 5),
                    (7, 7, 4),
                ]
            )
        )

        res = self.client.ann_search(
            test_collection_name,
            vec_data=[0.712338,0.603321,0.133444],
            vec_column_name="v",
            distance_func=l2_distance,
            topk=5,
            output_column_names=["c1", "c2", "c3"],
            where_clause=[text("c2 > 5 and c3 < 6")],
            idx_name_hint="idx1"
        )
        self.assertEqual(
            set(res.fetchall()),
            set(
                [
                    (9, 9, 2),
                    (10, 10, 1),
                    (8, 8, 3),
                    (6, 6, 5),
                    (7, 7, 4),
                ]
            )
        )
