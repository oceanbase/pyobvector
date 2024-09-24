"""A utility module for the extended data type class 'VECTOR'."""
import json
import numpy as np


class Vector:
    """A transformer class between python numpy array and OceanBase VECTOR.

    Attributes:
    _value (numpy.array) : a numpy array
    """
    def __init__(self, value):
        # big-endian float32
        if not isinstance(value, np.ndarray) or value.dtype != ">f4":
            value = np.asarray(value, dtype=">f4")

        if value.ndim != 1:
            raise ValueError(f"expected ndim to be 1: {value} {value.ndim}")

        self._value = value

    def __repr__(self):
        return f"{self._value.tolist()}"

    def dim(self):
        """Get vector dimension."""
        return len(self._value)

    def to_list(self):
        """Parse numpy array to python list."""
        return self._value.tolist()

    def to_numpy(self):
        """Get numpy array."""
        return self._value

    def to_text(self):
        """Parse numpy array to list string."""
        return "[" + ",".join([str(np.float32(v)) for v in self._value]) + "]"

    @classmethod
    def from_text(cls, value: str):
        """Construct Vector class with list string.

        Args:
            value: For example, '[1,2,3]'
        """
        return cls([float(v) for v in value[1:-1].split(",")])

    @classmethod
    def from_bytes(cls, value: bytes):
        """Construct Vector class with raw bytes.

        Args:
            value: the bytes of python list
        """
        return cls(json.loads(value.decode()))

    @classmethod
    def _to_db(cls, value, dim=None):
        if value is None:
            return value

        if not isinstance(value, cls):
            value = cls(value)

        if dim is not None and value.dim() != dim:
            raise ValueError(f"expected {dim} dimensions, not {value.dim()}")

        return value.to_text()

    @classmethod
    def _from_db(cls, value):
        if value is None or isinstance(value, np.ndarray):
            return value

        if isinstance(value, str):
            return cls.from_text(value).to_numpy().astype(np.float32)
        if isinstance(value, bytes):
            return cls.from_bytes(value).to_numpy().astype(np.float32)
        raise ValueError("unexpect vector type")
