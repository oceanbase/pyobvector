# pyobvector

A python SDK for OceanBase Vector Store, based on SQLAlchemy, compatible with Milvus API.

## Installation

- git clone this repo, then install with:

```shell
poetry install
```

- install with pip:

```shell
pip install pyobvector==0.1.8
```

## Build Doc

You can build document locally with `sphinx`:

```shell
mkdir build
make html
```

## Usage

`pyobvector` supports two modes:

- `Milvus compatible mode`: You can use the `MilvusLikeClient` class to use vector storage in a way similar to the Milvus API
- `SQLAlchemy hybrid mode`: You can use the vector storage function provided by the `ObVecClient` class and execute the relational database statement with the SQLAlchemy library. In this mode, you can regard `pyobvector` as an extension of SQLAlchemy.

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
# perform ann search
res = self.client.ann_search(
    test_collection_name, 
    vec_data=[0,0,0], 
    vec_column_name='embedding',
    distance_func=func.l2_distance,
    topk=5,
    output_column_names=['id']
)
# For example, the result will be:
# [(112,), (111,), (10,), (11,), (12,)]
```
