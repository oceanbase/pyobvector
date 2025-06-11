"""ARRAY: An extended data type for SQLAlchemy"""
import json
from typing import Any, List, Optional, Sequence, Union

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
