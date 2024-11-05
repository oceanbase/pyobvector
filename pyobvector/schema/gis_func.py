"""gis_func: An extended system function in GIS."""

import logging
from typing import Tuple

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy import BINARY, Float, Boolean, Text

from .geo_srid_point import POINT

logger = logging.getLogger(__name__)

class ST_GeomFromText(FunctionElement):
    """ST_GeomFromText: parse text to geometry object.
    
    Attributes:
    type : result type
    """
    type = BINARY()

    def __init__(self, *args):
        super().__init__()
        self.args = args

@compiles(ST_GeomFromText)
def compile_ST_GeomFromText(element, compiler, **kwargs): # pylint: disable=unused-argument
    """Compile ST_GeomFromText function."""
    args = []
    for idx, arg in enumerate(element.args):
        if idx == 0:
            if (
                (not isinstance(arg, Tuple)) or
                (len(arg) != 2) or
                (not all(isinstance(x, float) for x in arg))
            ):
                raise ValueError(
                    f"Tuple[float, float] is expected for Point literal," \
                    f"while get {type(arg)}"
                )
            args.append(f"'{POINT.to_db(arg)}'")
        else:
            args.append(str(arg))
    args_str = ", ".join(args)
    # logger.info(f"{args_str}")
    return f"ST_GeomFromText({args_str})"

class st_distance(FunctionElement):
    """st_distance: calculate distance between Points.
    
    Attributes:
    type : result type
    """
    type = Float()
    inherit_cache = True

    def __init__(self, *args):
        super().__init__()
        self.args = args

@compiles(st_distance)
def compile_st_distance(element, compiler, **kwargs): # pylint: disable=unused-argument
    """Compile st_distance function."""
    args = ", ".join(compiler.process(arg) for arg in element.args)
    return f"st_distance({args})"

class st_dwithin(FunctionElement):
    """st_dwithin: Checks if the distance between two points
    is less than a specified distance.
    
    Attributes:
    type : result type
    """
    type = Boolean()
    inherit_cache = True

    def __init__(self, *args):
        super().__init__()
        self.args = args

@compiles(st_dwithin)
def compile_st_dwithin(element, compiler, **kwargs): # pylint: disable=unused-argument
    """Compile st_dwithin function."""
    args = []
    for idx, arg in enumerate(element.args):
        if idx == 2:
            args.append(str(arg))
        else:
            args.append(compiler.process(arg))
    args_str = ", ".join(args)
    return f"_st_dwithin({args_str})"

class st_astext(FunctionElement):
    """st_astext: Returns a Point in human-readable format.

    Attributes:
    type : result type
    """
    type = Text()
    inherit_cache = True

    def __init__(self, *args):
        super().__init__()
        self.args = args

@compiles(st_astext)
def compile_st_astext(element, compiler, **kwargs): # pylint: disable=unused-argument
    """Compile st_astext function."""
    args = ", ".join(compiler.process(arg) for arg in element.args)
    return f"st_astext({args})"
