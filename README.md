# pyobvector

A python SDK for OceanBase Multimodal Store (Vector Store / Full Text Search / JSON Table), based on SQLAlchemy, compatible with Milvus API.

[![Downloads](https://static.pepy.tech/badge/pyobvector)](https://pepy.tech/project/pyobvector)  [![Downloads](https://static.pepy.tech/badge/pyobvector/month)](https://pepy.tech/project/pyobvector)

## Installation

- git clone this repo, then install with:

```shell
poetry install
```

- install with pip:

```shell
pip install pyobvector==0.2.19
```

## Build Doc

You can build document locally with `sphinx`:

```shell
mkdir build
make html
```

## Release Notes

For detailed release notes and changelog, see [RELEASE_NOTES.md](RELEASE_NOTES.md).

## Usage

`pyobvector` supports three modes:

- `Milvus compatible mode`: You can use the `MilvusLikeClient` class to use vector storage in a way similar to the Milvus API
- `SQLAlchemy hybrid mode`: You can use the vector storage function provided by the `ObVecClient` class and execute the relational database statement with the SQLAlchemy library. In this mode, you can regard `pyobvector` as an extension of SQLAlchemy.
- `Hybrid Search mode`: You can use the `HybridSearch` class to perform hybrid search that combines full-text search and vector similarity search, with Elasticsearch-compatible query syntax.

### Milvus compatible mode

Refer to `tests/test_milvus_like_client.py` for more examples.

A simple workflow to perform ANN search with OceanBase Vector Store:

- setup a client:

```python
from pyobvector import *

client = MilvusLikeClient(uri="127.0.0.1:2881", user="test@test")
```

- create a collection with vector index:

```python
test_collection_name = "ann_test"
# define the schema of collection with optional partitions
range_part = ObRangePartition(False, range_part_infos = [
    RangeListPartInfo('p0', 100),
    RangeListPartInfo('p1', 'maxvalue'),
], range_expr='id')
schema = client.create_schema(partitions=range_part)
# define field schema of collection
schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)
schema.add_field(field_name="meta", datatype=DataType.JSON, nullable=True)
# define index parameters
idx_params = self.client.prepare_index_params()
idx_params.add_index(
    field_name='embedding',
    index_type=VecIndexType.HNSW,
    index_name='vidx',
    metric_type="L2",
    params={"M": 16, "efConstruction": 256},
)
# create collection
client.create_collection(
    collection_name=test_collection_name,
    schema=schema,
    index_params=idx_params,
)
```

- insert data to your collection:

```python
# prepare
vector_value1 = [0.748479,0.276979,0.555195]
vector_value2 = [0, 0, 0]
data1 = [{'id': i, 'embedding': vector_value1} for i in range(10)]
data1.extend([{'id': i, 'embedding': vector_value2} for i in range(10, 13)])
data1.extend([{'id': i, 'embedding': vector_value2} for i in range(111, 113)])
# insert data
client.insert(collection_name=test_collection_name, data=data1)
```

- do ann search:

```python
res = client.search(collection_name=test_collection_name, data=[0,0,0], anns_field='embedding', limit=5, output_fields=['id'])
# For example, the result will be:
# [{'id': 112}, {'id': 111}, {'id': 10}, {'id': 11}, {'id': 12}]
```

### SQLAlchemy hybrid mode

- setup a client:

```python
from pyobvector import *
from sqlalchemy import Column, Integer, JSON
from sqlalchemy import func

client = ObVecClient(uri="127.0.0.1:2881", user="test@test")
```

- create a partitioned table with vector index:

```python
# create partitioned table
range_part = ObRangePartition(False, range_part_infos = [
    RangeListPartInfo('p0', 100),
    RangeListPartInfo('p1', 'maxvalue'),
], range_expr='id')

cols = [
    Column('id', Integer, primary_key=True, autoincrement=False),
    Column('embedding', VECTOR(3)),
    Column('meta', JSON)
]
client.create_table(test_collection_name, columns=cols, partitions=range_part)

# create vector index
client.create_index(
    test_collection_name, 
    is_vec_index=True, 
    index_name='vidx',
    column_names=['embedding'],
    vidx_params='distance=l2, type=hnsw, lib=vsag',
)
```

- insert data to your collection:

```python
# insert data
vector_value1 = [0.748479,0.276979,0.555195]
vector_value2 = [0, 0, 0]
data1 = [{'id': i, 'embedding': vector_value1} for i in range(10)]
data1.extend([{'id': i, 'embedding': vector_value2} for i in range(10, 13)])
data1.extend([{'id': i, 'embedding': vector_value2} for i in range(111, 113)])
client.insert(test_collection_name, data=data1)
```

- do ann search:

```python
# perform ann search with basic column selection
res = self.client.ann_search(
    test_collection_name, 
    vec_data=[0,0,0], 
    vec_column_name='embedding',
    distance_func=l2_distance,
    topk=5,
    output_column_names=['id']  # Legacy parameter
)
# For example, the result will be:
# [(112,), (111,), (10,), (11,), (12,)]

# perform ann search with SQLAlchemy expressions (recommended)
from sqlalchemy import Table, text, func

table = Table(test_collection_name, client.metadata_obj, autoload_with=client.engine)
res = self.client.ann_search(
    test_collection_name, 
    vec_data=[0,0,0], 
    vec_column_name='embedding',
    distance_func=l2_distance,
    topk=5,
    output_columns=[
        table.c.id,
        table.c.meta,
        (table.c.id + 1000).label('id_plus_1000'),
        text("JSON_EXTRACT(meta, '$.key') as extracted_key")
    ]
)
# For example, the result will be:
# [(112, '{"key": "value"}', 1112, 'value'), ...]

# perform ann search with distance threshold (filter results by distance)
res = self.client.ann_search(
    test_collection_name, 
    vec_data=[0,0,0], 
    vec_column_name='embedding',
    distance_func=l2_distance,
    with_dist=True,
    topk=10,
    output_column_names=['id'],
    distance_threshold=0.5  # Only return results where distance <= 0.5
)
# Only returns results with distance <= 0.5
# For example, the result will be:
# [(10, 0.0), (11, 0.0), ...]  # Only includes results with distance <= 0.5
```

#### ann_search Parameters

The `ann_search` method supports flexible output column selection through the `output_columns` parameter:

- **`output_columns`** (recommended): Accepts SQLAlchemy Column objects, expressions, or a mix of both

  - Column objects: `table.c.id`, `table.c.name`
  - Expressions: `(table.c.age + 10).label('age_plus_10')`
  - JSON queries: `text("JSON_EXTRACT(meta, '$.key') as extracted_key")`
  - String functions: `func.concat(table.c.name, ' (', table.c.age, ')').label('name_age')`
- **`output_column_names`** (legacy): Accepts list of column name strings

  - Example: `['id', 'name', 'meta']`
- **Parameter Priority**: `output_columns` takes precedence over `output_column_names` when both are provided
- **`distance_threshold`** (optional): Filter results by distance threshold

  - Type: `Optional[float]`
  - Only returns results where `distance <= threshold`
  - Example: `distance_threshold=0.5` returns only results with distance <= 0.5
  - Use case: Quality control for similarity search, only return highly similar results
- If you want to use pure `SQLAlchemy` API with `OceanBase` dialect, you can just get an `SQLAlchemy.engine` via `client.engine`. The engine can also be created as following:

```python
import pyobvector
from sqlalchemy.dialects import registry
from sqlalchemy import create_engine

uri: str = "127.0.0.1:2881"
user: str = "root@test"
password: str = ""
db_name: str = "test"
registry.register("mysql.oceanbase", "pyobvector.schema.dialect", "OceanBaseDialect")
connection_str = (
    f"mysql+oceanbase://{user}:{password}@{uri}/{db_name}?charset=utf8mb4"
)
engine = create_engine(connection_str, **kwargs)
```

- Async engine is also supported:

```python
import pyobvector
from sqlalchemy.dialects import registry
from sqlalchemy.ext.asyncio import create_async_engine

uri: str = "127.0.0.1:2881"
user: str = "root@test"
password: str = ""
db_name: str = "test"
registry.register("mysql.aoceanbase", "pyobvector", "AsyncOceanBaseDialect")
connection_str = (
    f"mysql+aoceanbase://{user}:{password}@{uri}/{db_name}?charset=utf8mb4"
)
engine = create_async_engine(connection_str)
```

- For further usage in pure `SQLAlchemy` mode, please refer to [SQLAlchemy](https://www.sqlalchemy.org/)

### Hybrid Search Mode

`pyobvector` supports hybrid search that combines full-text search and vector similarity search, with query syntax compatible with Elasticsearch. This allows you to perform semantic search with both keyword matching and vector similarity in a single query.

- setup a client:

```python
from pyobvector import *
from pyobvector.client.hybrid_search import HybridSearch
from sqlalchemy import Column, Integer, VARCHAR

client = HybridSearch(uri="127.0.0.1:2881", user="test@test")
```

**Note**: Hybrid search requires OceanBase version >= 4.4.1.0, or SeekDB.

- create a table with both vector index and full-text index:

```python
test_table_name = "hybrid_search_test"

# create table with vector and text columns
client.create_table(
    table_name=test_table_name,
    columns=[
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("source_id", VARCHAR(32)),
        Column("enabled", Integer),
        Column("vector", VECTOR(3)),  # vector column
        Column("title", VARCHAR(255)),  # text column for full-text search
        Column("content", VARCHAR(255)),  # text column for full-text search
    ],
    indexes=[
        VectorIndex("vec_idx", "vector", params="distance=l2, type=hnsw, lib=vsag"),
    ],
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
)

# create full-text indexes for text columns
from pyobvector import FtsIndexParam, FtsParser

for col in ["title", "content"]:
    client.create_fts_idx_with_fts_index_param(
        table_name=test_table_name,
        fts_idx_param=FtsIndexParam(
            index_name=f"fts_idx_{col}",
            field_names=[col],
            parser_type=FtsParser.IK,  # or other parser types
        ),
    )
```

- insert data:

```python
client.insert(
    table_name=test_table_name,
    data=[
        {
            "id": 1,
            "source_id": "3b767712b57211f09c170242ac130008",
            "enabled": 1,
            "vector": [1, 1, 1],
            "title": "企业版和社区版的功能差异",
            "content": "OceanBase 数据库提供企业版和社区版两种形态。",
        },
        {
            "id": 2,
            "vector": [1, 2, 3],
            "enabled": 1,
            "source_id": "3b791472b57211f09c170242ac130008",
            "title": "快速体验 OceanBase 社区版",
            "content": "本文根据使用场景详细介绍如何快速部署 OceanBase 数据库。",
        },
        # ... more data
    ]
)
```

- perform hybrid search with Elasticsearch-compatible query syntax:

```python
# build query body (compatible with Elasticsearch syntax)
query = {
    "bool": {
        "must": [
            {
                "query_string": {
                    "fields": ["title^10", "content"],  # field weights
                    "type": "best_fields",
                    "query": "oceanbase 数据 迁移",
                    "minimum_should_match": "30%",
                    "boost": 1
                }
            }
        ],
        "filter": [
            {
                "terms": {
                    "source_id": [
                        "3b791472b57211f09c170242ac130008",
                        "3b7af31eb57211f09c170242ac130008"
                    ]
                }
            },
            {
                "bool": {
                    "must_not": [
                        {
                            "range": {
                                "enabled": {"lt": 1}
                            }
                        }
                    ]
                }
            }
        ],
        "boost": 0.7
    }
}

body = {
    "query": query,
    "knn": {  # vector similarity search
        "field": "vector",
        "k": 1024,
        "num_candidates": 1024,
        "query_vector": [1, 2, 3],
        "filter": query,  # optional: apply same filter to KNN
        "similarity": 0.2  # similarity threshold
    },
    "from": 0,  # pagination offset
    "size": 60  # pagination size
}

# execute hybrid search
results = client.search(index=test_table_name, body=body)
# results is a list of matching documents
```

#### Supported Query Types

The hybrid search supports Elasticsearch-compatible query syntax:

- **`bool` query**: Combine multiple queries with `must`, `must_not`, `should`, `filter`
- **`query_string`**: Full-text search with field weights, boost, and matching options
- **`terms`**: Exact match filtering for multiple values
- **`range`**: Range queries (`lt`, `lte`, `gt`, `gte`)
- **`knn`**: Vector similarity search (KNN) with:
  - `field`: Vector field name
  - `query_vector`: Query vector
  - `k`: Number of results to return
  - `num_candidates`: Number of candidates to consider
  - `filter`: Optional filter to apply to KNN search
  - `similarity`: Similarity threshold
- **Pagination**: `from` and `size` parameters

#### Get SQL Query

You can also get the actual SQL that will be executed:

```python
sql = client.get_sql(index=test_table_name, body=body)
print(sql)  # prints the SQL query
```
