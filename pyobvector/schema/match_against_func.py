"""match_against_func: An extend system function in FTS."""

import logging

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy import BOOLEAN

logger = logging.getLogger(__name__)

class MatchAgaint(FunctionElement):
    """MatchAgaint: match clause for full text search.

    Attributes:
    type : result type
    """
    inherit_cache = True

    def __init__(self, *args):
        super().__init__()
        self.args = args
    
@compiles(MatchAgaint)
def complie_MatchAgaint(element, compiler, **kwargs): # pylint: disable=unused-argument
    """Compile MatchAgaint function."""
    args = element.args
    if len(args) < 2:
        raise ValueError(
            f"MatchAgaints should take a string expression and " \
            f"at least one column name string as parameters."
        )
    cols = ", ".join(args[1:])
    return f"MATCH ({cols}) AGAINST ('{args[0]}' IN NATURAL LANGUAGE MODE)"
