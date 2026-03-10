"""Multi-type Vector Store Client:

1. `Milvus compatible mode`: You can use the `MilvusLikeClient` class to use vector storage
in a way similar to the Milvus API.
2. `SQLAlchemy hybrid mode`: You can use the vector storage function provided by the
`ObVecClient` class and execute the relational database statement with the SQLAlchemy library.
In this mode, you can regard `pyobvector` as an extension of SQLAlchemy.
3. `Embedded SeekDB`: ObClient/ObVecClient support path= or pyseekdb_client= for embedded
SeekDB (pip install pyobvector[pyseekdb]). Same API as remote: create_table, insert, etc.

* SeekdbRemoteClient    Connect to embedded (path= / pyseekdb_client=) or remote; returns ObVecClient
* ObVecClient           MySQL/SeekDB client in SQLAlchemy hybrid mode (uri, path, or pyseekdb_client)
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
* FtsParser             Text Parser Type for Full Text Search
* FtsIndexParam         Full Text Search index parameter
"""

import os
from typing import Any

from .ob_vec_client import ObVecClient
from .milvus_like_client import MilvusLikeClient
from .ob_vec_json_table_client import ObVecJsonTableClient
from .index_param import VecIndexType, IndexParam, IndexParams
from .schema_type import DataType
from .collection_schema import FieldSchema, CollectionSchema
from .partitions import *
from .fts_index_param import FtsParser, FtsIndexParam


def _resolve_password(password: str) -> str:
    return password or os.environ.get("SEEKDB_PASSWORD", "")


def SeekdbRemoteClient(
    path: str | None = None,
    uri: str | None = None,
    host: str | None = None,
    port: int | None = None,
    tenant: str = "test",
    database: str = "test",
    user: str | None = None,
    password: str = "",
    pyseekdb_client: Any | None = None,
    **kwargs: Any,
) -> Any:
    """
    Connect to embedded SeekDB (path= or pyseekdb_client=) or remote OceanBase/SeekDB (uri/host=).
    Returns ObVecClient with the same API (create_table, insert, ann_search, etc.).
    Embedded requires: pip install pyobvector[pyseekdb]
    """
    password = _resolve_password(password)
    if pyseekdb_client is not None:
        return ObVecClient(pyseekdb_client=pyseekdb_client, **kwargs)
    if path is not None:
        return ObVecClient(path=path, db_name=database, **kwargs)
    if uri is None and host is not None:
        port = port if port is not None else 2881
        uri = f"{host}:{port}"
    if uri is None:
        uri = "127.0.0.1:2881"
    ob_user = user if user is not None else "root"
    if "@" not in ob_user:
        ob_user = f"{ob_user}@{tenant}"
    return ObVecClient(
        uri=uri,
        user=ob_user,
        password=password,
        db_name=database,
        **kwargs,
    )


__all__ = [
    "SeekdbRemoteClient",
    "ObVecClient",
    "MilvusLikeClient",
    "ObVecJsonTableClient",
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
    "FtsParser",
    "FtsIndexParam",
]
