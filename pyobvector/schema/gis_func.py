"""gis_func: An extended system function in GIS."""

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy import BINARY

class ST_GeomFromText(FunctionElement):
    """ST_GeomFromText: parse text to geometry object.
    
    Attributes:
    type : result type
    """
    type = BINARY()

@compiles(ST_GeomFromText)
def compile_ST_GeomFromText(element, compiler, **kwargs): # pylint: disable=unused-argument
    args = ", ".join(compiler.process(arg) for arg in element.clauses)
    return f"ST_GeomFromText({args})"