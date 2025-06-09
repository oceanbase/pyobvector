"""ARRAY: An extended data type for SQLAlchemy"""
import json
from typing import Any, List, Optional, Sequence, Union, Type

from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.types import UserDefinedType, String


class ARRAY(UserDefinedType):
    """ARRAY data type definition with support for up to 6 levels of nesting."""
    cache_ok = True
    _string = String()
    _max_nesting_level = 6

    def __init__(self, item_type: Union[TypeEngine, type]):
        """Construct an ARRAY.

        Args:
            item_type: The data type of items in this array. For nested arrays,
                      pass another ARRAY type.

        Raises:
            ValueError: If nesting level exceeds the maximum allowed level (6).
        """
        super(UserDefinedType, self).__init__()
        if isinstance(item_type, type):
            item_type = item_type()
        self.item_type = item_type
        self._validate_nesting_level()

    def _validate_nesting_level(self):
        """Validate that the nesting level does not exceed the maximum allowed level."""
        level = 1
        current_type = self.item_type
        while isinstance(current_type, ARRAY):
            level += 1
            if level > self._max_nesting_level:
                raise ValueError(f"Maximum nesting level of {self._max_nesting_level} exceeded")
            current_type = current_type.item_type

    def get_col_spec(self, **kw):  # pylint: disable=unused-argument
        """Parse to array data type definition in text SQL."""
        if hasattr(self.item_type, 'get_col_spec'):
            base_type = self.item_type.get_col_spec(**kw)
        else:
            base_type = str(self.item_type)
        return f"ARRAY({base_type})"

    def bind_processor(self, dialect):
        item_proc = self.item_type.dialect_impl(dialect).bind_processor(dialect)

        def process(value: Optional[Sequence[Any]]) -> Optional[str]:
            if value is None:
                return None

            def convert(val):
                if isinstance(val, (list, tuple)):
                    return [convert(v) for v in val]
                if item_proc:
                    return item_proc(val)
                return val

            processed = convert(value)
            return json.dumps(processed)

        return process

    def result_processor(self, dialect, coltype):
        item_proc = self.item_type.dialect_impl(dialect).result_processor(dialect, coltype)

        def process(value: Optional[str]) -> Optional[List[Any]]:
            if value is None:
                return None

            def convert(val):
                if isinstance(val, (list, tuple)):
                    return [convert(v) for v in val]
                if item_proc:
                    return item_proc(val)
                return val

            value = json.loads(value) if isinstance(value, str) else value
            return convert(value)

        return process

    def literal_processor(self, dialect):
        item_proc = self.item_type.dialect_impl(dialect).literal_processor(dialect)

        def process(value: Sequence[Any]) -> str:
            def convert(val):
                if isinstance(val, (list, tuple)):
                    return [convert(v) for v in val]
                if item_proc:
                    return item_proc(val)
                return val

            processed = convert(value)
            return json.dumps(processed)

        return process

    def __repr__(self):
        """Return a string representation of the array type."""
        current_type = self.item_type
        nesting_level = 1
        base_type = current_type

        # Find the innermost type and count nesting level
        while isinstance(current_type, ARRAY):
            nesting_level += 1
            current_type = current_type.item_type
            if not isinstance(current_type, ARRAY):
                base_type = current_type

        return f"{nesting_level}D_Array({base_type})"


def nested_array(dim: int) -> Type[ARRAY]:
    """Create a nested array type class with specified dimensions.
    
    Args:
        dim: The number of dimensions for the array type (1-6)
        
    Returns:
        A class type that can be instantiated with an item_type to create a nested array
        
    Raises:
        ValueError: If dim is not between 1 and 6
    """
    if not 1 <= dim <= 6:
        raise ValueError("Dimension must be between 1 and 6")

    class ArrayType(ARRAY):
        def __init__(self, item_type: Union[TypeEngine, type]):
            nested_type = item_type
            for _ in range(dim - 1):
                nested_type = ARRAY(nested_type)
            super().__init__(nested_type)

    ArrayType.__name__ = f"{dim}D_Array"
    return ArrayType
