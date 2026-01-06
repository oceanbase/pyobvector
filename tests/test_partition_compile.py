import unittest
from pyobvector import *
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ObPartitionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.maxDiff = None

    def test_range_and_range_columns_partition(self):
        range_part = ObRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", "maxvalue"),
            ],
            range_expr="id",
        )
        self.assertEqual(
            range_part.do_compile(),
            "PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (maxvalue))",
        )

        range_columns_part = ObRangePartition(
            True,
            range_part_infos=[
                RangeListPartInfo("M202001", "'2020/02/01'"),
                RangeListPartInfo("M202002", "'2020/03/01'"),
                RangeListPartInfo("M202003", "'2020/04/01'"),
                RangeListPartInfo("MMAX", "MAXVALUE"),
            ],
            col_name_list=["log_date"],
        )
        self.assertEqual(
            range_columns_part.do_compile(),
            "PARTITION BY RANGE COLUMNS (log_date) (PARTITION M202001 VALUES LESS THAN ('2020/02/01'),PARTITION M202002 VALUES LESS THAN ('2020/03/01'),PARTITION M202003 VALUES LESS THAN ('2020/04/01'),PARTITION MMAX VALUES LESS THAN (MAXVALUE))",
        )

    def test_list_and_list_columns_partition(self):
        list_part = ObListPartition(
            False,
            list_part_infos=[
                RangeListPartInfo("p0", [1, 2, 3]),
                RangeListPartInfo("p1", [5, 6]),
                RangeListPartInfo("p2", "DEFAULT"),
            ],
            list_expr="col1",
        )
        self.assertEqual(
            list_part.do_compile(),
            "PARTITION BY LIST (col1) (PARTITION p0 VALUES IN (1,2,3),PARTITION p1 VALUES IN (5,6),PARTITION p2 VALUES IN (DEFAULT))",
        )

        list_columns_part = ObListPartition(
            True,
            list_part_infos=[
                RangeListPartInfo("p0", ["'00'", "'01'"]),
                RangeListPartInfo("p1", ["'02'", "'03'"]),
                RangeListPartInfo("p2", "DEFAULT"),
            ],
            col_name_list=["partition_id"],
        )
        self.assertEqual(
            list_columns_part.do_compile(),
            "PARTITION BY LIST COLUMNS (partition_id) (PARTITION p0 VALUES IN ('00','01'),PARTITION p1 VALUES IN ('02','03'),PARTITION p2 VALUES IN (DEFAULT))",
        )

    def test_hash_and_key_partition(self):
        hash_part = ObHashPartition("col1", part_count=60)
        self.assertEqual(
            hash_part.do_compile(), "PARTITION BY HASH (col1) PARTITIONS 60"
        )

        key_part = ObKeyPartition(col_name_list=["id", "gmt_create"], part_count=10)
        self.assertEqual(
            key_part.do_compile(), "PARTITION BY KEY (id,gmt_create) PARTITIONS 10"
        )

    def test_range_columns_with_sub_partition(self):
        range_columns_part = ObRangePartition(
            True,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", 200),
                RangeListPartInfo("p2", 300),
            ],
            col_name_list=["col1"],
        )
        range_sub_part = ObSubRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("mp0", 1000),
                RangeListPartInfo("mp1", 2000),
                RangeListPartInfo("mp2", 3000),
            ],
            range_expr="col3",
        )
        range_columns_part.add_subpartition(range_sub_part)
        self.assertEqual(
            range_columns_part.do_compile(),
            "PARTITION BY RANGE COLUMNS (col1) SUBPARTITION BY RANGE (col3) SUBPARTITION TEMPLATE (SUBPARTITION mp0 VALUES LESS THAN (1000),SUBPARTITION mp1 VALUES LESS THAN (2000),SUBPARTITION mp2 VALUES LESS THAN (3000)) (PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (200),PARTITION p2 VALUES LESS THAN (300))",
        )

        range_columns_sub_part = ObSubRangePartition(
            True,
            range_part_infos=[
                RangeListPartInfo("mp0", 1000),
                RangeListPartInfo("mp1", 2000),
                RangeListPartInfo("mp2", 3000),
            ],
            col_name_list=["col2"],
        )
        range_columns_part.add_subpartition(range_columns_sub_part)
        self.assertEqual(
            range_columns_part.do_compile(),
            "PARTITION BY RANGE COLUMNS (col1) SUBPARTITION BY RANGE COLUMNS (col2) SUBPARTITION TEMPLATE (SUBPARTITION mp0 VALUES LESS THAN (1000),SUBPARTITION mp1 VALUES LESS THAN (2000),SUBPARTITION mp2 VALUES LESS THAN (3000)) (PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (200),PARTITION p2 VALUES LESS THAN (300))",
        )

        list_columns_sub_part = ObSubListPartition(
            True,
            list_part_infos=[
                RangeListPartInfo("mp0", [1, 3]),
                RangeListPartInfo("mp1", [4, 6]),
                RangeListPartInfo("mp2", [7]),
            ],
            col_name_list=["col2"],
        )
        range_columns_part.add_subpartition(list_columns_sub_part)
        self.assertEqual(
            range_columns_part.do_compile(),
            "PARTITION BY RANGE COLUMNS (col1) SUBPARTITION BY LIST COLUMNS (col2) SUBPARTITION TEMPLATE (SUBPARTITION mp0 VALUES IN (1,3),SUBPARTITION mp1 VALUES IN (4,6),SUBPARTITION mp2 VALUES IN (7)) (PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (200),PARTITION p2 VALUES LESS THAN (300))",
        )

        hash_part = ObSubHashPartition("col2", part_count=5)
        range_columns_part.add_subpartition(hash_part)
        self.assertEqual(
            range_columns_part.do_compile(),
            "PARTITION BY RANGE COLUMNS (col1) SUBPARTITION BY HASH (col2) SUBPARTITIONS 5 (PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (200),PARTITION p2 VALUES LESS THAN (300))",
        )

        key_part = ObSubKeyPartition(["col2"], part_count=3)
        range_columns_part.add_subpartition(key_part)
        self.assertEqual(
            range_columns_part.do_compile(),
            "PARTITION BY RANGE COLUMNS (col1) SUBPARTITION BY KEY (col2) SUBPARTITIONS 3 (PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (200),PARTITION p2 VALUES LESS THAN (300))",
        )

    def test_list_with_sub_partitions(self):
        list_part = ObListPartition(
            False,
            list_part_infos=[
                RangeListPartInfo("p0", [1, 3]),
                RangeListPartInfo("p1", [4, 6]),
                RangeListPartInfo("p2", [7, 9]),
            ],
            list_expr="col1",
        )
        range_sub_part = ObSubRangePartition(
            False,
            range_part_infos=[
                RangeListPartInfo("mp0", 100),
                RangeListPartInfo("mp1", 200),
                RangeListPartInfo("mp2", 300),
            ],
            range_expr="col2",
        )
        list_part.add_subpartition(range_sub_part)
        self.assertEqual(
            list_part.do_compile(),
            "PARTITION BY LIST (col1) SUBPARTITION BY RANGE (col2) SUBPARTITION TEMPLATE (SUBPARTITION mp0 VALUES LESS THAN (100),SUBPARTITION mp1 VALUES LESS THAN (200),SUBPARTITION mp2 VALUES LESS THAN (300)) (PARTITION p0 VALUES IN (1,3),PARTITION p1 VALUES IN (4,6),PARTITION p2 VALUES IN (7,9))",
        )

        hash_sub_part = ObSubHashPartition("col3", part_count=3)
        list_part.add_subpartition(hash_sub_part)
        self.assertEqual(
            list_part.do_compile(),
            "PARTITION BY LIST (col1) SUBPARTITION BY HASH (col3) SUBPARTITIONS 3 (PARTITION p0 VALUES IN (1,3),PARTITION p1 VALUES IN (4,6),PARTITION p2 VALUES IN (7,9))",
        )

        key_sub_part = ObSubKeyPartition(["col3"], part_count=3)
        list_part.add_subpartition(key_sub_part)
        self.assertEqual(
            list_part.do_compile(),
            "PARTITION BY LIST (col1) SUBPARTITION BY KEY (col3) SUBPARTITIONS 3 (PARTITION p0 VALUES IN (1,3),PARTITION p1 VALUES IN (4,6),PARTITION p2 VALUES IN (7,9))",
        )

    def test_list_columns_with_sub_partitions(self):
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
        # logging.info(list_columns_part.do_compile())
        self.assertEqual(
            list_columns_part.do_compile(),
            "PARTITION BY LIST COLUMNS (col1) SUBPARTITION BY RANGE COLUMNS (col2) SUBPARTITION TEMPLATE (SUBPARTITION mp0 VALUES LESS THAN (100),SUBPARTITION mp1 VALUES LESS THAN (200),SUBPARTITION mp2 VALUES LESS THAN (300)) (PARTITION p0 VALUES IN (1,3),PARTITION p1 VALUES IN (4,6),PARTITION p2 VALUES IN (7,9))",
        )

        list_columns_sub_part = ObSubListPartition(
            True,
            list_part_infos=[
                RangeListPartInfo("mp0", 2),
                RangeListPartInfo("mp1", 5),
                RangeListPartInfo("mp2", 8),
            ],
            col_name_list=["col2"],
        )
        list_columns_part.add_subpartition(list_columns_sub_part)
        self.assertEqual(
            list_columns_part.do_compile(),
            "PARTITION BY LIST COLUMNS (col1) SUBPARTITION BY LIST COLUMNS (col2) SUBPARTITION TEMPLATE (SUBPARTITION mp0 VALUES IN (2),SUBPARTITION mp1 VALUES IN (5),SUBPARTITION mp2 VALUES IN (8)) (PARTITION p0 VALUES IN (1,3),PARTITION p1 VALUES IN (4,6),PARTITION p2 VALUES IN (7,9))",
        )

        hash_sub_part = ObSubHashPartition("col2", part_count=5)
        list_columns_part.add_subpartition(hash_sub_part)
        self.assertEqual(
            list_columns_part.do_compile(),
            "PARTITION BY LIST COLUMNS (col1) SUBPARTITION BY HASH (col2) SUBPARTITIONS 5 (PARTITION p0 VALUES IN (1,3),PARTITION p1 VALUES IN (4,6),PARTITION p2 VALUES IN (7,9))",
        )

        key_sub_part = ObSubKeyPartition(["col2"], part_count=3)
        list_columns_part.add_subpartition(key_sub_part)
        self.assertEqual(
            list_columns_part.do_compile(),
            "PARTITION BY LIST COLUMNS (col1) SUBPARTITION BY KEY (col2) SUBPARTITIONS 3 (PARTITION p0 VALUES IN (1,3),PARTITION p1 VALUES IN (4,6),PARTITION p2 VALUES IN (7,9))",
        )

    def test_hash_with_sub_partitions(self):
        hash_part = ObHashPartition("col1", part_count=5)
        range_columns_sub_part = ObSubRangePartition(
            True,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", 200),
                RangeListPartInfo("p2", 300),
            ],
            col_name_list=["col2"],
        )
        hash_part.add_subpartition(range_columns_sub_part)
        # logging.info(hash_part.do_compile())
        self.assertEqual(
            hash_part.do_compile(),
            "PARTITION BY HASH (col1) SUBPARTITION BY RANGE COLUMNS (col2) SUBPARTITION TEMPLATE (SUBPARTITION p0 VALUES LESS THAN (100),SUBPARTITION p1 VALUES LESS THAN (200),SUBPARTITION p2 VALUES LESS THAN (300)) PARTITIONS 5",
        )

        list_columns_sub_part = ObSubListPartition(
            True,
            list_part_infos=[
                RangeListPartInfo("p0", [1, 3]),
                RangeListPartInfo("p1", [4, 6]),
                RangeListPartInfo("p2", [7, 9]),
            ],
            col_name_list=["col2"],
        )
        hash_part.add_subpartition(list_columns_sub_part)
        # logging.info(hash_part.do_compile())
        self.assertEqual(
            hash_part.do_compile(),
            "PARTITION BY HASH (col1) SUBPARTITION BY LIST COLUMNS (col2) SUBPARTITION TEMPLATE (SUBPARTITION p0 VALUES IN (1,3),SUBPARTITION p1 VALUES IN (4,6),SUBPARTITION p2 VALUES IN (7,9)) PARTITIONS 5",
        )

        hash_sub_part = ObSubHashPartition(
            "col2", hash_part_name_list=["sp0", "sp1", "sp2"]
        )
        hash_part.add_subpartition(hash_sub_part)
        # logging.info(hash_part.do_compile())
        self.assertEqual(
            hash_part.do_compile(),
            "PARTITION BY HASH (col1) SUBPARTITION BY HASH (col2) SUBPARTITION TEMPLATE (SUBPARTITION sp0,SUBPARTITION sp1,SUBPARTITION sp2) PARTITIONS 5",
        )

        key_sub_part = ObSubKeyPartition(
            ["col2"], key_part_name_list=["sp0", "sp1", "sp2"]
        )
        hash_part.add_subpartition(key_sub_part)
        # logging.info(hash_part.do_compile())
        self.assertEqual(
            hash_part.do_compile(),
            "PARTITION BY HASH (col1) SUBPARTITION BY KEY (col2) SUBPARTITION TEMPLATE (SUBPARTITION sp0,SUBPARTITION sp1,SUBPARTITION sp2) PARTITIONS 5",
        )

    def test_key_with_sub_partitions(self):
        key_part = ObKeyPartition(["col1"], key_part_name_list=["p0", "p1", "p2"])
        range_columns_sub_part = ObSubRangePartition(
            True,
            range_part_infos=[
                RangeListPartInfo("p0", 100),
                RangeListPartInfo("p1", 200),
                RangeListPartInfo("p2", 300),
            ],
            col_name_list=["col2"],
        )
        key_part.add_subpartition(range_columns_sub_part)
        self.assertEqual(
            key_part.do_compile(),
            "PARTITION BY KEY (col1) SUBPARTITION BY RANGE COLUMNS (col2) SUBPARTITION TEMPLATE (SUBPARTITION p0 VALUES LESS THAN (100),SUBPARTITION p1 VALUES LESS THAN (200),SUBPARTITION p2 VALUES LESS THAN (300)) (PARTITION p0,PARTITION p1,PARTITION p2)",
        )

        list_columns_sub_part = ObSubListPartition(
            True,
            list_part_infos=[
                RangeListPartInfo("p0", [1, 3]),
                RangeListPartInfo("p1", [4, 6]),
                RangeListPartInfo("p2", [7, 9]),
            ],
            col_name_list=["col2"],
        )
        key_part.add_subpartition(list_columns_sub_part)
        self.assertEqual(
            key_part.do_compile(),
            "PARTITION BY KEY (col1) SUBPARTITION BY LIST COLUMNS (col2) SUBPARTITION TEMPLATE (SUBPARTITION p0 VALUES IN (1,3),SUBPARTITION p1 VALUES IN (4,6),SUBPARTITION p2 VALUES IN (7,9)) (PARTITION p0,PARTITION p1,PARTITION p2)",
        )

        key_sub_part = ObSubKeyPartition(
            ["col2"], key_part_name_list=["sp0", "sp1", "sp2"]
        )
        key_part.add_subpartition(key_sub_part)
        self.assertEqual(
            key_part.do_compile(),
            "PARTITION BY KEY (col1) SUBPARTITION BY KEY (col2) SUBPARTITION TEMPLATE (SUBPARTITION sp0,SUBPARTITION sp1,SUBPARTITION sp2) (PARTITION p0,PARTITION p1,PARTITION p2)",
        )


if __name__ == "__main__":
    unittest.main()
