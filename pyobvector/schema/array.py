"""ARRAY: An extended data type for SQLAlchemy"""
import json
from typing import Any, List, Optional, Sequence, Union, Type

from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.types import UserDefinedType, String


class ARRAY(UserDefinedType):
    """ARRAY data type definition with support for up to 6 levels of nesting."""
    cache_ok = True
    _string = String()

    def __init__(self, item_type: Union[TypeEngine, type]):
        """Construct an ARRAY.

        Args:
            item_type: The data type of items in this array. For nested arrays,
                      pass another ARRAY type.
        """
        super(UserDefinedType, self).__init__()
        if isinstance(item_type, type):
            item_type = item_type()
        self.item_type = item_type
        if isinstance(item_type, ARRAY):
            self.dim = item_type.dim + 1
        else:
            self.dim = 1
        if self.dim > 6:
            raise ValueError("Maximum nesting level of 6 exceeded")

    def get_col_spec(self, **kw):  # pylint: disable=unused-argument
        """Parse to array data type definition in text SQL."""
        if hasattr(self.item_type, 'get_col_spec'):
            base_type = self.item_type.get_col_spec(**kw)
        else:
            base_type = str(self.item_type)
        return f"ARRAY({base_type})"

    def _get_list_depth(self, value: Any) -> int:
        if not isinstance(value, list):
            return 0
        max_depth = 0
        for element in value:
            current_depth = self._get_list_depth(element)
            if current_depth > max_depth:
                max_depth = current_depth
        return 1 + max_depth

    def _validate_dimension(self, value: list[Any]):
        arr_depth = self._get_list_depth(value)
        assert arr_depth == self.dim, "Array dimension mismatch, expected {}, got {}".format(self.dim, arr_depth)

    def bind_processor(self, dialect):
        item_type = self.item_type
        while isinstance(item_type, ARRAY):
            item_type = item_type.item_type

        item_proc = item_type.dialect_impl(dialect).bind_processor(dialect)

        def process(value: Optional[Sequence[Any] | str]) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, str):
                self._validate_dimension(json.loads(value))
                return value

            def convert(val):
                if isinstance(val, (list, tuple)):
                    return [convert(v) for v in val]
                if item_proc:
                    return item_proc(val)
                return val

            processed = convert(value)
            self._validate_dimension(processed)
            return json.dumps(processed)

        return process

    def result_processor(self, dialect, coltype):
        item_type = self.item_type
        while isinstance(item_type, ARRAY):
            item_type = item_type.item_type

        item_proc = item_type.dialect_impl(dialect).result_processor(dialect, coltype)

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
        item_type = self.item_type
        while isinstance(item_type, ARRAY):
            item_type = item_type.item_type

        item_proc = item_type.dialect_impl(dialect).literal_processor(dialect)

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

    class NestedArray(ARRAY):
        cache_ok = True
        _string = String()

        def __init__(self, item_type: Union[TypeEngine, type]):
            super(UserDefinedType, self).__init__()
            if isinstance(item_type, type):
                item_type = item_type()

            assert not isinstance(item_type, ARRAY), "The item_type of NestedArray should not be an ARRAY type"

            nested_type = item_type
            for _ in range(dim):
                nested_type = ARRAY(nested_type)

            self.item_type = nested_type.item_type
            self.dim = dim

    return NestedArray
