"""ReplaceStmt: replace into statement compilation."""
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

class ReplaceStmt(Insert):
    """Replace into statement."""
    inherit_cache = True

@compiles(ReplaceStmt)
def compile_replace_stmt(insert, compiler, **kw):
    """Compile replace into statement.

    Args:
        insert: replace clause
        compiler: SQL compiler
    """
    stmt_str = compiler.visit_insert(insert, **kw)
    stmt_str = stmt_str.replace("INSERT INTO", "REPLACE INTO")
    return stmt_str
