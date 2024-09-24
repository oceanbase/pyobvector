"""A module to do compilation of OceanBase Parition Clause."""
from typing import List, Optional, Union
import logging
from dataclasses import dataclass
from .enum import IntEnum
from .exceptions import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PartType(IntEnum):
    """Partition type of table or collection for both ObVecClient and MilvusLikeClient"""
    Range = 0
    Hash = 1
    Key = 2
    List = 3
    RangeColumns = 4
    ListColumns = 5


class ObPartition:
    """Base class of all kind of Partition strategy
    
    Attributes:
    part_type (PartType) : type of partition strategy
    sub_partition (ObPartition) : subpartition strategy
    is_sub (bool) : this partition strategy is a subpartition or not
    """
    def __init__(self, part_type: PartType):
        self.part_type = part_type
        self.sub_partition = None
        self.is_sub = False

    def do_compile(self) -> str:
        """Compile partition strategy to text SQL."""
        raise NotImplementedError()

    def add_subpartition(self, sub_part):
        """Add subpartition strategy to current partition.
        
        Args:
            sub_part (ObPartition) : subpartition strategy
        """
        if self.is_sub:
            raise PartitionFieldException(
                code=ErrorCode.INVALID_ARGUMENT,
                message=ExceptionsMessage.PartitionLevelMoreThanTwo,
            )

        if sub_part is None:
            return

        if not sub_part.is_sub:
            raise ValueError("not a subparition")

        self.sub_partition = sub_part


@dataclass
class RangeListPartInfo:
    """Range/RangeColumns/List/ListColumns partition info for each partition.
    
    Attributes:
    part_name (string) : partition name
    part_upper_bound_expr (Union[List, str, int]) : 
        For example, using `[1,2]`/`'DEFAULT'` as default case/`7` when create 
        List/ListColumns partition.
        Using 100 / `MAXVALUE` when create Range/RangeColumns partition.
    """
    part_name: str
    part_upper_bound_expr: Union[List, str, int]

    def get_part_expr_str(self):
        """Parse part_upper_bound_expr to text SQL."""
        if isinstance(self.part_upper_bound_expr, List):
            return ",".join([str(v) for v in self.part_upper_bound_expr])
        if isinstance(self.part_upper_bound_expr, str):
            return self.part_upper_bound_expr
        if isinstance(self.part_upper_bound_expr, int):
            return str(self.part_upper_bound_expr)
        raise ValueError("Invalid datatype")


class ObRangePartition(ObPartition):
    """Range/RangeColumns partition strategy."""
    def __init__(
        self,
        is_range_columns: bool,
        range_part_infos: List[RangeListPartInfo],
        range_expr: Optional[str] = None,
        col_name_list: Optional[List[str]] = None,
    ):
        super().__init__(PartType.RangeColumns if is_range_columns else PartType.Range)
        self.range_part_infos = range_part_infos
        self.range_expr = range_expr
        self.col_name_list = col_name_list

        if not is_range_columns and range_expr is None:
            raise PartitionFieldException(
                code=ErrorCode.INVALID_ARGUMENT,
                message=ExceptionsMessage.PartitionRangeExprMissing,
            )

        if is_range_columns and col_name_list is None:
            raise PartitionFieldException(
                code=ErrorCode.INVALID_ARGUMENT,
                message=ExceptionsMessage.PartitionRangeColNameListMissing,
            )

    def do_compile(self) -> str:
        """Compile partition strategy to text SQL."""
        return f"PARTITION BY {self._compile_helper()}"

    def _compile_helper(self) -> str:
        if self.part_type == PartType.Range:
            assert self.range_expr is not None
            if self.sub_partition is None:
                return f"RANGE ({self.range_expr}) ({self._parse_range_part_list()})"
            return f"RANGE ({self.range_expr}) {self.sub_partition.do_compile()} " \
                    f"({self._parse_range_part_list()})"
        assert self.col_name_list is not None
        if self.sub_partition is None:
            return f"RANGE COLUMNS ({','.join(self.col_name_list)}) " \
                    f"({self._parse_range_part_list()})"
        return f"RANGE COLUMNS ({','.join(self.col_name_list)}) " \
                f"{self.sub_partition.do_compile()} ({self._parse_range_part_list()})"

    def _parse_range_part_list(self) -> str:
        range_partitions_complied = [
            f"PARTITION {range_part_info.part_name} VALUES LESS THAN " \
            f"({range_part_info.get_part_expr_str()})"
            for range_part_info in self.range_part_infos
        ]
        return ",".join(range_partitions_complied)


class ObSubRangePartition(ObRangePartition):
    """Range/RangeColumns subpartition strategy."""
    def __init__(
        self,
        is_range_columns: bool,
        range_part_infos: List[RangeListPartInfo],
        range_expr: Optional[str] = None,
        col_name_list: Optional[List[str]] = None,
    ):
        super().__init__(is_range_columns, range_part_infos, range_expr, col_name_list)
        self.is_sub = True

    def do_compile(self) -> str:
        """Compile partition strategy to text SQL."""
        return f"SUBPARTITION BY {self._compile_helper()}"

    def _compile_helper(self) -> str:
        if self.part_type == PartType.Range:
            assert self.range_expr is not None
            assert self.sub_partition is None
            return f"RANGE ({self.range_expr}) SUBPARTITION TEMPLATE " \
                   f"({self._parse_range_part_list()})"
        assert self.col_name_list is not None
        assert self.sub_partition is None
        return f"RANGE COLUMNS ({','.join(self.col_name_list)}) SUBPARTITION TEMPLATE " \
                f"({self._parse_range_part_list()})"

    def _parse_range_part_list(self) -> str:
        range_partitions_complied = [
            f"SUBPARTITION {range_part_info.part_name} VALUES LESS THAN " \
            f"({range_part_info.get_part_expr_str()})"
            for range_part_info in self.range_part_infos
        ]
        return ",".join(range_partitions_complied)


class ObListPartition(ObPartition):
    """List/ListColumns partition strategy."""
    def __init__(
        self,
        is_list_columns: bool,
        list_part_infos: List[RangeListPartInfo],
        list_expr: Optional[str] = None,
        col_name_list: Optional[List[str]] = None,
    ):
        super().__init__(PartType.ListColumns if is_list_columns else PartType.List)
        self.list_part_infos = list_part_infos
        self.list_expr = list_expr
        self.col_name_list = col_name_list

        if not is_list_columns and list_expr is None:
            raise PartitionFieldException(
                code=ErrorCode.INVALID_ARGUMENT,
                message=ExceptionsMessage.PartitionListExprMissing,
            )

        if is_list_columns and col_name_list is None:
            raise PartitionFieldException(
                code=ErrorCode.INVALID_ARGUMENT,
                message=ExceptionsMessage.PartitionListColNameListMissing,
            )

    def do_compile(self) -> str:
        """Compile partition strategy to text SQL."""
        return f"PARTITION BY {self._compile_helper()}"

    def _compile_helper(self) -> str:
        if self.part_type == PartType.List:
            assert self.list_expr is not None
            if self.sub_partition is None:
                return f"LIST ({self.list_expr}) ({self._parse_list_part_list()})"
            return f"LIST ({self.list_expr}) {self.sub_partition.do_compile()} " \
                    f"({self._parse_list_part_list()})"
        assert self.col_name_list is not None
        if self.sub_partition is None:
            return f"LIST COLUMNS ({','.join(self.col_name_list)}) " \
                    f"({self._parse_list_part_list()})"
        return f"LIST COLUMNS ({','.join(self.col_name_list)}) " \
                f"{self.sub_partition.do_compile()} ({self._parse_list_part_list()})"

    def _parse_list_part_list(self) -> str:
        list_partitions_complied = [
            f"PARTITION {list_part_info.part_name} VALUES IN ({list_part_info.get_part_expr_str()})"
            for list_part_info in self.list_part_infos
        ]
        return ",".join(list_partitions_complied)


class ObSubListPartition(ObListPartition):
    """List/ListColumns subpartition strategy."""
    def __init__(
        self,
        is_list_columns: bool,
        list_part_infos: List[RangeListPartInfo],
        list_expr: Optional[str] = None,
        col_name_list: Optional[List[str]] = None,
    ):
        super().__init__(is_list_columns, list_part_infos, list_expr, col_name_list)
        self.is_sub = True

    def do_compile(self) -> str:
        """Compile partition strategy to text SQL."""
        return f"SUBPARTITION BY {self._compile_helper()}"

    def _compile_helper(self) -> str:
        if self.part_type == PartType.List:
            assert self.list_expr is not None
            assert self.sub_partition is None
            return f"LIST ({self.list_expr}) SUBPARTITION TEMPLATE ({self._parse_list_part_list()})"
        assert self.col_name_list is not None
        assert self.sub_partition is None
        return f"LIST COLUMNS ({','.join(self.col_name_list)}) SUBPARTITION TEMPLATE " \
                f"({self._parse_list_part_list()})"

    def _parse_list_part_list(self) -> str:
        list_partitions_complied = [
            f"SUBPARTITION {list_part_info.part_name} VALUES IN " \
            f"({list_part_info.get_part_expr_str()})"
            for list_part_info in self.list_part_infos
        ]
        return ",".join(list_partitions_complied)


class ObHashPartition(ObPartition):
    """Hash partition strategy."""
    def __init__(
        self,
        hash_expr: str,
        hash_part_name_list: List[str] = None,
        part_count: Optional[int] = None,
    ):
        super().__init__(PartType.Hash)
        self.hash_expr = hash_expr
        self.hash_part_name_list = hash_part_name_list
        self.part_count = part_count

        if self.hash_part_name_list is None and self.part_count is None:
            raise PartitionFieldException(
                code=ErrorCode.INVALID_ARGUMENT,
                message=ExceptionsMessage.PartitionHashNameListAndPartCntMissing,
            )

        if self.part_count is not None and self.hash_part_name_list is not None:
            logging.warning(
                "part_count & hash_part_name_list are both set, " \
                "hash_part_name_list will be override by part_count"
            )

    def do_compile(self) -> str:
        """Compile partition strategy to text SQL."""
        return f"PARTITION BY {self._compile_helper()}"

    def _compile_helper(self) -> str:
        if self.part_count is not None:
            if self.sub_partition is None:
                return f"HASH ({self.hash_expr}) PARTITIONS {self.part_count}"
            return f"HASH ({self.hash_expr}) {self.sub_partition.do_compile()} " \
                    f"PARTITIONS {self.part_count}"
        assert self.hash_part_name_list is not None
        if self.sub_partition is None:
            return f"HASH ({self.hash_expr}) ({self._parse_hash_part_list()})"
        return f"HASH ({self.hash_expr}) {self.sub_partition.do_compile()} " \
                f"({self._parse_hash_part_list()})"

    def _parse_hash_part_list(self):
        return ",".join([f"PARTITION {name}" for name in self.hash_part_name_list])


class ObSubHashPartition(ObHashPartition):
    """Hash subpartition strategy."""
    def __init__(
        self,
        hash_expr: str,
        hash_part_name_list: List[str] = None,
        part_count: Optional[int] = None,
    ):
        super().__init__(hash_expr, hash_part_name_list, part_count)
        self.is_sub = True

    def do_compile(self) -> str:
        """Compile partition strategy to text SQL."""
        return f"SUBPARTITION BY {self._compile_helper()}"

    def _compile_helper(self) -> str:
        if self.part_count is not None:
            assert self.sub_partition is None
            return f"HASH ({self.hash_expr}) SUBPARTITIONS {self.part_count}"
        assert self.hash_part_name_list is not None
        assert self.sub_partition is None
        return f"HASH ({self.hash_expr}) SUBPARTITION TEMPLATE ({self._parse_hash_part_list()})"

    def _parse_hash_part_list(self):
        return ",".join([f"SUBPARTITION {name}" for name in self.hash_part_name_list])


class ObKeyPartition(ObPartition):
    """Key partition strategy."""
    def __init__(
        self,
        col_name_list: List[str],
        key_part_name_list: List[str] = None,
        part_count: Optional[int] = None,
    ):
        super().__init__(PartType.Key)
        self.col_name_list = col_name_list
        self.key_part_name_list = key_part_name_list
        self.part_count = part_count

        if self.key_part_name_list is None and self.part_count is None:
            raise PartitionFieldException(
                code=ErrorCode.INVALID_ARGUMENT,
                message=ExceptionsMessage.PartitionKeyNameListAndPartCntMissing,
            )

        if self.part_count is not None and self.key_part_name_list is not None:
            logging.warning(
                "part_count & key_part_name_list are both set, " \
                "key_part_name_list will be override by part_count"
            )

    def do_compile(self) -> str:
        """Compile partition strategy to text SQL."""
        return f"PARTITION BY {self._compile_helper()}"

    def _compile_helper(self) -> str:
        if self.part_count is not None:
            if self.sub_partition is None:
                return (
                    f"KEY ({','.join(self.col_name_list)}) PARTITIONS {self.part_count}"
                )
            return f"KEY ({','.join(self.col_name_list)}) {self.sub_partition.do_compile()} " \
                    f"PARTITIONS {self.part_count}"
        assert self.key_part_name_list is not None
        if self.sub_partition is None:
            return f"KEY ({','.join(self.col_name_list)}) ({self._parse_key_part_list()})"
        return f"KEY ({','.join(self.col_name_list)}) {self.sub_partition.do_compile()} " \
                f"({self._parse_key_part_list()})"

    def _parse_key_part_list(self):
        return ",".join([f"PARTITION {name}" for name in self.key_part_name_list])


class ObSubKeyPartition(ObKeyPartition):
    """Key subpartition strategy."""
    def __init__(
        self,
        col_name_list: List[str],
        key_part_name_list: List[str] = None,
        part_count: Optional[int] = None,
    ):
        super().__init__(col_name_list, key_part_name_list, part_count)
        self.is_sub = True

    def do_compile(self) -> str:
        """Compile partition strategy to text SQL."""
        return f"SUBPARTITION BY {self._compile_helper()}"

    def _compile_helper(self) -> str:
        if self.part_count is not None:
            assert self.sub_partition is None
            return (
                f"KEY ({','.join(self.col_name_list)}) SUBPARTITIONS {self.part_count}"
            )
        assert self.key_part_name_list is not None
        assert self.sub_partition is None
        return f"KEY ({','.join(self.col_name_list)}) SUBPARTITION TEMPLATE " \
                f"({self._parse_key_part_list()})"

    def _parse_key_part_list(self):
        return ",".join([f"SUBPARTITION {name}" for name in self.key_part_name_list])
