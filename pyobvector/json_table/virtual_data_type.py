from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from enum import Enum
from typing import Optional
from typing_extensions import Annotated

from pydantic import BaseModel, Field, AfterValidator, create_model


class IntEnum(int, Enum):
    """Int type enumerate definition."""

class JType(IntEnum):
    J_BOOL = 1
    J_TIMESTAMP = 2
    J_VARCHAR = 3
    J_DECIMAL = 4
    J_INT = 5

class JsonTableDataType(BaseModel):
    type: JType

class JsonTableBool(JsonTableDataType):
    type: JType = Field(default=JType.J_BOOL)
    val: Optional[bool]

class JsonTableTimestamp(JsonTableDataType):
    type: JType = Field(default=JType.J_TIMESTAMP)
    val: Optional[datetime]

def check_varchar_len_with_length(length: int):
    def check_varchar_len(x: Optional[str]):
        if x is None:
            return None
        if len(x) > length:
            raise ValueError(f'{x} is longer than {length}')
        return x
    
    return check_varchar_len

class JsonTableVarcharFactory:
    def __init__(self, length: int):
        self.length = length

    def get_json_table_varchar_type(self):
        model_name = f"JsonTableVarchar{self.length}"
        fields = {
            'type': (JType, JType.J_VARCHAR),
            'val': (Annotated[Optional[str], AfterValidator(check_varchar_len_with_length(self.length))], ...)
        }
        return create_model(
            model_name,
            __base__=JsonTableDataType,
            **fields
        )

def check_and_parse_decimal(x: int, y: int):
    def check_float(v):
        if v is None:
            return None
        try:
            decimal_value = Decimal(v)
        except InvalidOperation:
            raise ValueError(f"Value {v} cannot be converted to Decimal.")
        
        decimal_str = str(decimal_value).strip()
    
        if '.' in decimal_str:
            integer_part, decimal_part = decimal_str.split('.')
        else:
            integer_part, decimal_part = decimal_str, ''
    
        integer_count = len(integer_part.lstrip('-'))  # 去掉负号的长度
        decimal_count = len(decimal_part)

        if integer_count + min(decimal_count, y) > x:
            raise ValueError(f"'{v}' Range out of Decimal({x}, {y})")
        
        if decimal_count > y:
            quantize_str = '1.' + '0' * y
            decimal_value = decimal_value.quantize(Decimal(quantize_str), rounding=ROUND_DOWN)
        return decimal_value
    return check_float

class JsonTableDecimalFactory:
    def __init__(self, ndigits: int, decimal_p: int):
        self.ndigits = ndigits
        self.decimal_p = decimal_p
    
    def get_json_table_decimal_type(self):
        model_name = f"JsonTableDecimal_{self.ndigits}_{self.decimal_p}"
        fields = {
            'type': (JType, JType.J_DECIMAL),
            'val': (Annotated[Optional[float], AfterValidator(check_and_parse_decimal(self.ndigits, self.decimal_p))], ...)
        }
        return create_model(
            model_name,
            __base__=JsonTableDataType,
            **fields
        )

class JsonTableInt(JsonTableDataType):
    type: JType = Field(default=JType.J_INT)
    val: Optional[int]

def val2json(val):
    if val is None:
        return None
    if isinstance(val, int) or isinstance(val, bool) or isinstance(val, str):
        return val
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)