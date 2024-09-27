"""Exception for MilvusLikeClient."""
from .enum import IntEnum


class ErrorCode(IntEnum):
    """Error codes for MilvusLikeClient."""
    SUCCESS = 0
    UNEXPECTED_ERROR = 1
    INVALID_ARGUMENT = 2
    NOT_SUPPORTED = 3
    COLLECTION_NOT_FOUND = 100
    INDEX_NOT_FOUND = 700


class ObException(Exception):
    """Base class for MilvusLikeClient exception."""
    def __init__(
        self,
        code: int = ErrorCode.UNEXPECTED_ERROR,
        message: str = "",
    ) -> None:
        super().__init__()
        self._code = code
        self._message = message

    @property
    def code(self):
        """Get error code."""
        return self._code

    @property
    def message(self):
        """Get error message."""
        return self._message

    def __str__(self) -> str:
        return f"<{type(self).__name__}: (code={self.code}, message={self.message})>"


class PartitionFieldException(ObException):
    """Raise when partition field invalid"""


class PrimaryKeyException(ObException):
    """Raise when primary key are invalid"""


class VectorFieldParamException(ObException):
    """Raise when Vector Field parameters are invalid"""


class VarcharFieldParamException(ObException):
    """Raise when Varchar Field parameters are invalid"""


class ArrayFieldParamException(ObException):
    """Raise when Array Field parameters are invalid"""


class CollectionStatusException(ObException):
    """Raise when collection status is invalid"""


class VectorMetricTypeException(ObException):
    """Raise when vector metric type is invalid"""


class MilvusCompatibilityException(ObException):
    """Raise when compatibility conflict with milvus"""


class ClusterVersionException(ObException):
    """Raise when cluster version is not valid"""


class ExceptionsMessage:
    """Exception Messages definition."""
    PartitionExprNotExists = "Partition expression string does not exist."
    PartitionMultiField = "Multi-Partition Field is not supported."
    PartitionLevelMoreThanTwo = "Partition Level should less than or equal to 2."
    PartitionRangeCutNotIncreasing = (
        "Range cut list should be monotonically increasing."
    )
    PartitionRangeExprMissing = (
        "Range expression is necessary when partition type is Range"
    )
    PartitionRangeColNameListMissing = (
        "Column name list is necessary when parititon type is RangeColumns"
    )
    PartitionListExprMissing = (
        "List expression is necessary when partition type is List"
    )
    PartitionListColNameListMissing = (
        "Column name list is necessary when parititon type is ListColumns"
    )
    PartitionHashNameListAndPartCntMissing = (
        "One of hash_part_name_list and part_count must be set when partition type is Hash"
    )
    PartitionKeyNameListAndPartCntMissing = (
        "One of key_part_name_list and part_count must be set when partition type is Key"
    )
    PrimaryFieldType = "Param primary_field must be int or str type."
    VectorFieldMissingDimParam = "Param 'dim' must be set for vector field."
    VarcharFieldMissinglengthParam = "Param 'max_length' must be set for varchar field."
    ArrayFiledMissingElementType = "Param 'element_type' must be set for array field."
    ArrayFiledInvalidElementType = (
        "Param 'element_type' can not be array/vector/varchar."
    )
    CollectionNotExists = "Collection does not exist."
    MetricTypeParamTypeInvalid = "MetricType param type should be string."
    MetricTypeValueInvalid = "MetricType should be 'l2'/'ip' in ann search."
    UsingInIDsWhenMultiPrimaryKey = "Using 'ids' when table has multi primary key."
    ClusterVersionIsLow = (
        "OceanBase Vector Store is not supported because cluster version is below 4.3.3.0."
    )
