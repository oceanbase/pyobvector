"""SPARSE_VECTOR: An extended data type for SQLAlchemy"""
from sqlalchemy.types import UserDefinedType, String
from ..util import SparseVector

class SPARSE_VECTOR(UserDefinedType):
    """SPARSE_VECTOR data type definition."""
    cache_ok = True
    _string = String()

    def __init__(self):
        super(UserDefinedType, self).__init__()

    def get_col_spec(self, **kw): # pylint: disable=unused-argument
        """Parse to sparse vector data type definition in text SQL."""
        return "SPARSEVECTOR"
    
    def bind_processor(self, dialect):
        def process(value):
            return SparseVector._to_db(value)

        return process
    
    def literal_processor(self, dialect):
        string_literal_processor = self._string._cached_literal_processor(dialect)

        def process(value):
            return string_literal_processor(SparseVector._to_db(value))

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return SparseVector._from_db(value)

        return process