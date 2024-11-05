"""A extension for SQLAlchemy for vector storage related schema definition.

* VECTOR            An extended data type in SQLAlchemy for ObVecClient
* VectorIndex       An extended index type in SQLAlchemy for ObVecClient
* CreateVectorIndex Vector Index Creation statement clause
* ObTable           Extension to Table for creating table with vector index
* l2_distance       New system function to calculate l2 distance between vectors
* cosine_distance   New system function to calculate cosine distance between vectors
* inner_product     New system function to calculate inner distance between vectors
* negative_inner_product
                    New system function to calculate neg ip distance between vectors
* ST_GeomFromText   GIS function: parse text to geometry object
* st_distance       GIS function: calculate distance between Points
* st_dwithin        GIS function: check if the distance between two points
* st_astext         GIS function: return a Point in human-readable format
* ReplaceStmt       Replace into statement based on the extension of SQLAlchemy.Insert
"""
from .vector import VECTOR
from .geo_srid_point import POINT
from .vector_index import VectorIndex, CreateVectorIndex
from .ob_table import ObTable
from .vec_dist_func import l2_distance, cosine_distance, inner_product, negative_inner_product
from .gis_func import ST_GeomFromText, st_distance, st_dwithin, st_astext
from .replace_stmt import ReplaceStmt
from .dialect import OceanBaseDialect, AsyncOceanBaseDialect

__all__ = [
    "VECTOR",
    "POINT",
    "VectorIndex",
    "CreateVectorIndex",
    "ObTable",
    "l2_distance",
    "cosine_distance",
    "inner_product",
    "negative_inner_product",
    "ST_GeomFromText",
    "st_distance",
    "st_dwithin",
    "st_astext",
    "ReplaceStmt",
    "OceanBaseDialect",
    "AsyncOceanBaseDialect",
]
