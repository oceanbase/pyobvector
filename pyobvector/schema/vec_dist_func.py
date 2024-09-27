"""vec_dist_func: An extended system function for SQLAlchemy.

The system function to calculate distance between vectors.
"""
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy import Float


class l2_distance(FunctionElement):
    """Vector distance function: l2_distance.
    
    Attributes:
    type : result type
    """
    type = Float()


@compiles(l2_distance)
def compile_l2_distance(element, compiler, **kwargs): # pylint: disable=unused-argument
    """Compile l2_distance function.

    Args:
        element: l2_distance arguments
        compiler: SQL compiler
    """
    args = ", ".join(compiler.process(arg) for arg in element.clauses)
    return f"l2_distance({args})"


class cosine_distance(FunctionElement):
    """Vector distance function: cosine_distance.
    
    Attributes:
    type : result type
    """
    type = Float()


@compiles(cosine_distance)
def compile_cosine_distance(element, compiler, **kwargs): # pylint: disable=unused-argument
    """Compile cosine_distance function.

    Args:
        element: cosine_distance arguments
        compiler: SQL compiler
    """
    args = ", ".join(compiler.process(arg) for arg in element.clauses)
    return f"cosine_distance({args})"


class inner_product(FunctionElement):
    """Vector distance function: inner_product.
    
    Attributes:
    type : result type
    """
    type = Float()


@compiles(inner_product)
def compile_inner_product(element, compiler, **kwargs): # pylint: disable=unused-argument
    """Compile inner_product function.

    Args:
        element: inner_product arguments
        compiler: SQL compiler
    """
    args = ", ".join(compiler.process(arg) for arg in element.clauses)
    return f"inner_product({args})"

class negative_inner_product(FunctionElement):
    """Vector distance function: negative_inner_product.
    
    Attributes:
    type : result type
    """
    type = Float()

@compiles(negative_inner_product)
def compile_negative_inner_product(element, compiler, **kwargs): # pylint: disable=unused-argument
    """Compile negative_inner_product function.

    Args:
        element: negative_inner_product arguments
        compiler: SQL compiler
    """
    args = ", ".join(compiler.process(arg) for arg in element.clauses)
    return f"negative_inner_product({args})"
