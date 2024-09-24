"""Multi-type Vector Store Client:

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
"""
from .ob_vec_client import ObVecClient
from .milvus_like_client import MilvusLikeClient
from .index_param import VecIndexType, IndexParam, IndexParams
from .schema_type import DataType
from .collection_schema import FieldSchema, CollectionSchema
from .partitions import *

__all__ = [
    "ObVecClient",
    "MilvusLikeClient",
    "VecIndexType",
    "IndexParam",
    "IndexParams",
    "DataType",
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
]
