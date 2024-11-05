"""A python SDK for OceanBase Vector Store, based on SQLAlchemy, compatible with Milvus API.

`pyobvector` supports two modes: 
1. `Milvus compatible mode`: You can use the `MilvusLikeClient` class to use vector storage 
in a way similar to the Milvus API.
2. `SQLAlchemy hybrid mode`: You can use the vector storage function provided by the 
`ObVecClient` class and execute the relational database statement with the SQLAlchemy library. 
In this mode, you can regard `pyobvector` as an extension of SQLAlchemy.

* ObVecClient           MySQL client in SQLAlchemy hybrid mode
* MilvusLikeClient      Milvus compatible client
* VecIndexType          VecIndexType is used to specify vector index type for MilvusLikeClient
* IndexParam            Specify vector index parameters for MilvusLikeClient
* IndexParams           A list of IndexParam to create vector index in batch
* DataType              Specify field type in collection schema for MilvusLikeClient
* VECTOR                An extended data type in SQLAlchemy for ObVecClient
* VectorIndex           An extended index type in SQLAlchemy for ObVecClient
* FieldSchema           Clas to define field schema in collection for MilvusLikeClient
* CollectionSchema      Class to define collection schema for MilvusLikeClient
* PartType              Specify partition type of table or collection 
                        for both ObVecClient and MilvusLikeClient
* ObPartition           Abstract type class of all kind of Partition strategy
* RangeListPartInfo     Specify Range/RangeColumns/List/ListColumns partition info
                        for each partition
* ObRangePartition      Specify Range/RangeColumns partition info
* ObSubRangePartition   Specify Range subpartition info
* ObListPartition       Specify List partition info
* ObSubListPartition    Specify List subpartition info
* ObHashPartition       Specify Hash partition info
* ObSubHashPartition    Specify Hash subpartition info
* ObKeyPartition        Specify Key partition info
* ObSubKeyPartition     Specify Key subpartition info
* ST_GeomFromText       GIS function: parse text to geometry object
* st_distance           GIS function: calculate distance between Points
* st_dwithin            GIS function: check if the distance between two points
* st_astext             GIS function: return a Point in human-readable format
"""
from .client import *
from .schema import (
    VECTOR,
    POINT,
    VectorIndex,
    OceanBaseDialect,
    AsyncOceanBaseDialect,
    ST_GeomFromText,
    st_distance,
    st_dwithin,
    st_astext,
)

__all__ = [
    "ObVecClient",
    "MilvusLikeClient",
    "VecIndexType",
    "IndexParam",
    "IndexParams",
    "DataType",
    "VECTOR",
    "POINT",
    "VectorIndex",
    "OceanBaseDialect",
    "AsyncOceanBaseDialect",
    "FieldSchema",
    "CollectionSchema",
    "PartType",
    "ObPartition",
    "RangeListPartInfo",
    "ObRangePartition",
    "ObSubRangePartition",
    "ObListPartition",
    "ObSubListPartition",
    "ObHashPartition",
    "ObSubHashPartition",
    "ObKeyPartition",
    "ObSubKeyPartition",
    "ST_GeomFromText",
    "st_distance",
    "st_dwithin",
    "st_astext",
]
