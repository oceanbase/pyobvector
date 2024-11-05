"""Point: OceanBase GIS data type for SQLAlchemy"""
from typing import Tuple, Optional
from sqlalchemy.types import UserDefinedType, String

class POINT(UserDefinedType):
    """Point data type definition."""
    cache_ok = True
    _string = String()

    def __init__(
        self,
        # lat_long: Tuple[float, float],
        srid: Optional[int] = None
    ):
        """Init Latitude and Longitude."""
        super(UserDefinedType, self).__init__()
        # self.lat_long = lat_long
        self.srid = srid

    def get_col_spec(self, **kw): # pylint: disable=unused-argument
        """Parse to Point data type definition in text SQL."""
        if self.srid is None:
            return "POINT"
        return f"POINT SRID {self.srid}"

    @classmethod
    def to_db(cls, value: Tuple[float, float]):
        """Parse tuple to POINT literal"""
        return f"POINT({value[0]} {value[1]})"

    def bind_processor(self, dialect):
        raise ValueError("Never access Point directly.")

    def literal_processor(self, dialect):
        raise ValueError("Never access Point directly.")

    def result_processor(self, dialect, coltype):
        raise ValueError("Never access Point directly.")
