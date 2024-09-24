"""FieldSchema & CollectionSchema definition module to be compatible with Milvus."""
import copy
from typing import Optional, List
from sqlalchemy import Column
from .schema_type import DataType, convert_datatype_to_sqltype
from .exceptions import *
from .partitions import *

class FieldSchema:
    """FieldSchema definition.

    Attributes:
    name (string) : field name
    dtype (DataType) : field data type
    description (string) : field description (not used in OceanBase)
    is_primary (bool) : whether the field is a primary column or not
    auto_id (bool) : whether the field is auto incremented
    nullable (bool) : whether the field can be null
    type_params (dict) : different parameters for different data type
    """
    def __init__(
        self,
        name: str,
        dtype: DataType,
        description: str = "",  # ignored in oceanbase
        is_primary: bool = False,
        auto_id: bool = False,
        nullable: bool = False,
        # is_partition_key: bool = False,  # different from Milvus
        # partition_expr_str: Optional[str] = None,  # different from Milvus
        **kwargs,
    ) -> None:
        self.name = name
        self.dtype = dtype
        self.description = description
        self.is_primary = is_primary
        self.auto_id = auto_id
        self.nullable = nullable
        self.column_schema = None
        self.kwargs = kwargs
        self.type_params = {}
        self._check_primary_key_datatype()
        self._parse_type_params()

    def _check_primary_key_datatype(self):
        if not self.is_primary:
            return
        if self.dtype in (
            DataType.STRING,
            DataType.ARRAY,
            DataType.JSON,
            DataType.FLOAT_VECTOR,
            DataType.DOUBLE,
            DataType.FLOAT,
        ):
            raise PrimaryKeyException(
                code=ErrorCode.INVALID_ARGUMENT,
                message=ExceptionsMessage.PrimaryFieldType,
            )

    def _parse_type_params(self):
        if self.dtype not in (
            DataType.FLOAT_VECTOR,
            DataType.VARCHAR,
            DataType.ARRAY,
        ):
            return
        if not self.kwargs:
            return

        if self.dtype == DataType.FLOAT_VECTOR:
            if "dim" not in self.kwargs:
                raise VectorFieldParamException(
                    code=ErrorCode.INVALID_ARGUMENT,
                    message=ExceptionsMessage.VectorFieldMissingDimParam,
                )
            self.type_params["dim"] = self.kwargs["dim"]
        elif self.dtype == DataType.VARCHAR:
            if "max_length" not in self.kwargs:
                raise VarcharFieldParamException(
                    code=ErrorCode.INVALID_ARGUMENT,
                    message=ExceptionsMessage.VarcharFieldMissinglengthParam,
                )
            self.type_params["length"] = self.kwargs["max_length"]
        elif self.dtype == DataType.ARRAY:
            if "element_type" not in self.kwargs:
                raise ArrayFieldParamException(
                    code=ErrorCode.INVALID_ARGUMENT,
                    message=ExceptionsMessage.ArrayFiledMissingElementType,
                )
            if self.kwargs["element_type"] in (
                DataType.ARRAY,
                DataType.FLOAT_VECTOR,
                DataType.VARCHAR,
            ):
                raise ArrayFieldParamException(
                    code=ErrorCode.INVALID_ARGUMENT,
                    message=ExceptionsMessage.ArrayFiledInvalidElementType,
                )

            self.type_params["item_type"] = convert_datatype_to_sqltype(
                self.kwargs["element_type"]
            )
            if "max_capacity" in self.kwargs:
                self.type_params["dimensions"] = self.kwargs["max_capacity"]

    def parse_to_sql_column(self):
        """Parse field schema to SQLAlchemy column schema."""
        self.column_schema = Column(
            self.name,
            convert_datatype_to_sqltype(self.dtype)(**self.type_params),
            primary_key=self.is_primary,
            autoincrement=self.auto_id,
            nullable=self.nullable,
        )


class CollectionSchema:
    """CollectionSchema definition.
    
    Attributes:
    fields (List[FieldSchema]) : a list of FieldSchema
    description (string) : collection description (not used in OceanBase)
    partitions (ObPartition) : partition strategy of this collection
    """
    def __init__(
        self,
        fields: Optional[List[FieldSchema]] = None,
        partitions: Optional[ObPartition] = None,
        description: str = "",  # ignored in oceanbase
        **kwargs,
    ):
        self.kwargs = copy.deepcopy(kwargs)
        self.description = description
        if fields is not None:
            self.fields = [copy.deepcopy(field) for field in fields]
        else:
            self.fields = []
        self.partitions = partitions
        self._check_fields()

    def _check_fields(self):
        for field in self.fields:
            field.parse_to_sql_column()

    def add_field(self, field_name: str, datatype: DataType, **kwargs):
        """Add field to collection.

        Args:
        :param field_name (string) : new field name
        :param datatype (DataType) : field data type
        :param kwargs : parameters for data type
        """
        field = FieldSchema(field_name, datatype, **kwargs)
        cur_idx = len(self.fields)
        self.fields.append(field)
        self.fields[cur_idx].parse_to_sql_column()
