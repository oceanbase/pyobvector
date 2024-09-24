"""VECTOR: An extended data type for SQLAlchemy"""
from sqlalchemy.types import UserDefinedType, String
from ..util import Vector


class VECTOR(UserDefinedType):
    """VECTOR data type definition."""
    cache_ok = True
    _string = String()

    def __init__(self, dim=None):
        super(UserDefinedType, self).__init__()
        self.dim = dim

    def get_col_spec(self, **kw): # pylint: disable=unused-argument
        """Parse to vector data type definition in text SQL."""
        if self.dim is None:
            return "VECTOR"
        return f"VECTOR({self.dim})"

    def bind_processor(self, dialect):
        def process(value):
            return Vector._to_db(value, self.dim)

        return process

    def literal_processor(self, dialect):
        string_literal_processor = self._string._cached_literal_processor(dialect)

        def process(value):
            return string_literal_processor(Vector._to_db(value, self.dim))

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return Vector._from_db(value)

        return process
