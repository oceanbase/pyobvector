"""A extension for SQLAlchemy for vector storage related schema definition.

* VECTOR            An extended data type in SQLAlchemy for ObVecClient
* VectorIndex       An extended index type in SQLAlchemy for ObVecClient
* CreateVectorIndex Vector Index Creation statement clause
* ObTable           Extension to Table for creating table with vector index
* l2_distance       New system function to calculate l2 distance between vectors
* cosine_distance   New system function to calculate cosine distance between vectors
* inner_product     New system function to calculate inner distance between vectors
"""
from .vector import VECTOR
from .vector_index import VectorIndex, CreateVectorIndex
from .ob_table import ObTable
from .vec_dist_func import l2_distance, cosine_distance, inner_product

__all__ = [
    "VECTOR",
    "VectorIndex",
    "CreateVectorIndex",
    "ObTable",
    "l2_distance",
    "cosine_distance",
    "inner_product",
]
