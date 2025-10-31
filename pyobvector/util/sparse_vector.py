"""A utility module for the extended data type class 'SPARSE_VECTOR'."""
import ast

class SparseVector:
    """A transformer class between python dict and OceanBase SPARSE_VECTOR.

    Attributes:
    _value (Dict) : a python dict
    """
    def __init__(self, value):
        if not isinstance(value, dict):
            raise ValueError("Sparse Vector should be a dict in python")
        
        self._value = value
    
    def __repr__(self):
        return f"{self._value}"
    
    def to_text(self):
        return f"{self._value}"
    
    @classmethod
    def from_text(cls, value: str):
        """Construct Sparse Vector class with dict in string format.

        Args:
            value: For example, '{1:1.1, 2:2.2}'
        """
        return cls(ast.literal_eval(value))
    
    @classmethod
    def _to_db(cls, value):
        if value is None:
            return value

        if not isinstance(value, cls):
            value = cls(value)

        return value.to_text()

    @classmethod
    def _from_db(cls, value):
        if value is None or isinstance(value, dict):
            return value

        if isinstance(value, str):
            return cls.from_text(value)._value
        raise ValueError(f"unexpected sparse vector type: {type(value)}")