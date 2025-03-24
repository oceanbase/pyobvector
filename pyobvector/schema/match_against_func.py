"""match_against_func: An extend system function in FTS."""

import logging

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy import literal, column

logger = logging.getLogger(__name__)

class MatchAgainst(FunctionElement):
    """MatchAgainst: match clause for full text search.

    Attributes:
    type : result type
    """
    inherit_cache = True

    def __init__(self, query, *columns):
        columns = [column(col) if isinstance(col, str) else col for col in columns]
        super().__init__(literal(query), *columns)
    
@compiles(MatchAgainst)
def complie_MatchAgainst(element, compiler, **kwargs): # pylint: disable=unused-argument
    """Compile MatchAgainst function."""
    clauses = list(element.clauses)
    if len(clauses) < 2:
        raise ValueError(
            f"MatchAgainst should take a string expression and " \
            f"at least one column name string as parameters."
        )
    
    query_expr = clauses[0]
    compiled_query = compiler.process(query_expr, **kwargs)
    column_exprs = clauses[1:]
    compiled_columns = [compiler.process(col, identifier_prepared=True) for col in column_exprs]
    columns_str = ", ".join(compiled_columns)
    return f"MATCH ({columns_str}) AGAINST ({compiled_query} IN NATURAL LANGUAGE MODE)"
