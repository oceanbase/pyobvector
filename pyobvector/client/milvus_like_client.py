"""Milvus Like Client."""
import logging
import json
from typing import Optional, Union, Dict, List

from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import (
    Column,
    Integer,
    String,
    text,
    Table,
    delete,
    select,
    and_,
)
from sqlalchemy.sql import func
import numpy as np

from .ob_vec_client import ObVecClient as Client
from .schema_type import DataType
from .collection_schema import CollectionSchema
from .index_param import IndexParams
from .exceptions import *
from ..schema import VECTOR, VectorIndex
from ..util import Vector

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MilvusLikeClient(Client):
    """Milvus Like Vector Database Client"""

    def __init__(
        self,
        uri: str = "127.0.0.1:2881",
        user: str = "root@test",
        password: str = "",
        db_name: str = "test",
        **kwargs,
    ):
        super().__init__(uri, user, password, db_name, **kwargs)

    # Collection & Schema API

    def create_schema(self, **kwargs) -> CollectionSchema:
        """Create a CollectionSchema object."""
        return CollectionSchema(**kwargs)

    def create_collection(
        self,
        collection_name: str,
        dimension: Optional[int] = None,
        primary_field_name: str = "id",
        id_type: Union[DataType, str] = DataType.INT64,
        vector_field_name: str = "vector",
        metric_type: str = "l2",
        auto_id: bool = False,
        timeout: Optional[float] = None,
        schema: Optional[CollectionSchema] = None,  # Used for custom setup
        index_params: Optional[IndexParams] = None,  # Used for custom setup
        max_length: int = 16384,
        **kwargs,
    ): # pylint: disable=unused-argument
        """Create a collection. 
        If `schema` is not None, `dimension`, `primary_field_name`, `id_type`, `vector_field_name`,
        `metric_type`, `auto_id` will be ignored.
        
        Args:
            collection_name (string): collection name
            dimension (Optional[int]): vector data dimension
            primary_field_name (string): primary field name
            id_type (Union[DataType, str]): primary field data type (Only VARCHAR and INT type supported)
            vector_field_name (string): vector field name
            metric_type (str): not used in OceanBase (for default, l2 distance)
            auto_id (bool): whether primary field is auto incremented
            timeout (Optional[float]): not used in OceanBase
            schema (Optional[CollectionSchema]): custom collection schema, when `schema` is not None
                the above argument will be ignored
            index_params (Optional[IndexParams]): custom vector index parameters
            max_length (int): when primary field data type is VARCHAR and `schema` is not None,
                the max varchar length is `max_length`
        """
        if isinstance(id_type, str):
            if id_type not in ("str", "string", "int", "integer"):
                raise PrimaryKeyException(
                    code=ErrorCode.INVALID_ARGUMENT,
                    message=ExceptionsMessage.PrimaryFieldType,
                )
            if id_type in ("str", "string"):
                id_type = DataType.VARCHAR
            else:
                id_type = DataType.INT64

        if id_type not in (
            DataType.VARCHAR,
            DataType.INT64,
            DataType.INT32,
            DataType.INT16,
            DataType.INT8,
            DataType.BOOL,
        ):
            raise PrimaryKeyException(
                code=ErrorCode.INVALID_ARGUMENT,
                message=ExceptionsMessage.PrimaryFieldType,
            )

        if schema is None:
            if dimension is None:
                raise VectorFieldParamException(
                    code=ErrorCode.INVALID_ARGUMENT,
                    message=ExceptionsMessage.VectorFieldMissingDimParam,
                )
            id_column = (
                Column(
                    primary_field_name,
                    Integer(),
                    primary_key=True,
                    autoincrement=auto_id,
                )
                if id_type == DataType.INT64
                else Column(
                    primary_field_name,
                    String(max_length),
                    primary_key=True,
                    autoincrement=auto_id,
                )
            )
            vector_column = Column(vector_field_name, VECTOR(dimension))
            columns = [id_column, vector_column]
            self.create_table_with_index_params(
                table_name=collection_name,
                columns=columns,
                indexes=None,
                vidxs=index_params,
            )
        else:
            columns = [field.column_schema for field in schema.fields]
            self.create_table_with_index_params(
                table_name=collection_name,
                columns=columns,
                indexes=None,
                vidxs=index_params,
                partitions=schema.partitions,
            )

    def get_collection_stats(
        self, collection_name: str, timeout: Optional[float] = None # pylint: disable=unused-argument
    ) -> Dict:
        """Get collection row count.
        
        Args:
            collection_name (string): collection name
            timeout (Optional[float]): not used in OceanBase

        Returns:
            dict: {'row_count': count}
        """
        with self.engine.connect() as conn:
            with conn.begin():
                res = conn.execute(
                    text(f"SELECT COUNT(*) as row_count FROM `{collection_name}`")
                )
                cnt = [r[0] for r in res][0]
                return {"row_count": cnt}

    def has_collection(
        self, collection_name: str, timeout: Optional[float] = None # pylint: disable=unused-argument
    ) -> bool: # pylint: disable=unused-argument
        """Check if collection exists.

        Args:
            collection_name (string): collection name
            timeout (Optional[float]): not used in OceanBase

        Returns:
            bool: True if collection exists else False
        """
        return self.check_table_exists(collection_name)

    def drop_collection(self, collection_name: str) -> None:
        """Drop collection if exists.
        
        Args:
            collection_name (string): collection name
        """
        self.drop_table_if_exist(collection_name)

    def rename_collection(
        self, old_name: str, new_name: str, timeout: Optional[float] = None # pylint: disable=unused-argument
    ) -> None:
        """Rename collection.
        
        Args:
            old_name (string): old collection name
            new_name (string): new collection name
            timeout (Optional[float]): not used in OceanBase
        """
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"RENAME TABLE `{old_name}` TO `{new_name}`"))

    def load_table(
        self,
        collection_name: str,
    ):
        """Load table into SQLAlchemy metadata.
        
        Args:
            collection_name (string): which collection to load

        Returns:
            sqlalchemy.Table: table object
        """
        try:
            table = Table(collection_name, self.metadata_obj, autoload_with=self.engine)
        except NoSuchTableError as e:
            raise CollectionStatusException(
                code=ErrorCode.COLLECTION_NOT_FOUND,
                message=ExceptionsMessage.CollectionNotExists,
            ) from e
        return table

    # Index API

    def create_index(
        self,
        collection_name: str,
        index_params: IndexParams,
        timeout: Optional[float] = None,
        **kwargs,
    ): # pylint: disable=unused-argument
        """Create vector index with index params.
        
        Args:
            collection_name (string): which collection to create vector index
            index_params (IndexParams): the vector index parameters
            timeout (Optional[float]): not used in OceanBase
            **kwargs: different args for different vector index type
        """
        try:
            table = Table(collection_name, self.metadata_obj, autoload_with=self.engine)
        except NoSuchTableError as e:
            raise CollectionStatusException(
                code=ErrorCode.COLLECTION_NOT_FOUND,
                message=ExceptionsMessage.CollectionNotExists,
            ) from e

        for index_param in index_params:
            with self.engine.connect() as conn:
                with conn.begin():
                    vidx = VectorIndex(
                        index_param.index_name,
                        table.c[index_param.field_name],
                        params=index_param.param_str(),
                    )
                    vidx.create(self.engine, checkfirst=True)

    def drop_index(
        self,
        collection_name: str,
        index_name: str,
        timeout: Optional[float] = None,
        **kwargs,
    ): # pylint: disable=unused-argument
        """Drop index on specified collection.
        
        If the index not exists, SQL ERROR 1091 will raise.

        Args:
            collection_name (string): which collection the index belongs to
            index_name (string): which index
            timeout (Optional[float]): not used in OceanBase
            **kwargs: additional arguments
        """
        super().drop_index(collection_name, index_name)

    def refresh_index(
        self,
        collection_name: str,
        index_name: str,
        trigger_threshold: int = 10000,
    ):
        """Refresh vector index for performance.
        
        Args:
            collection_name (string): collection name
            index_name (string): vector index name
            trigger_threshold (int): If delta_buffer_table row count is greater than `trigger_threshold`,
                refreshing is actually triggered.
        """
        super().refresh_index(
            table_name=collection_name,
            index_name=index_name,
            trigger_threshold=trigger_threshold,
        )

    def rebuild_index(
        self,
        collection_name: str,
        index_name: str,
        trigger_threshold: float = 0.2,
    ):
        """Rebuild vector index for performance.
        
        Args:
            collection_name (string): collection name
            index_name (string): vector index name
            trigger_threshold (float): threshold value for rebuilding index
        """
        super().rebuild_index(
            table_name=collection_name,
            index_name=index_name,
            trigger_threshold=trigger_threshold,
        )

    # Insert & Search

    def _parse_metric_type_str_to_dist_func(self, metric_type: str):
        if metric_type == "l2":
            return func.l2_distance
        if metric_type == "cosine":
            return func.cosine_distance
        if metric_type == "ip":
            return func.inner_product
        if metric_type == "neg_ip":
            return func.negative_inner_product
        raise VectorMetricTypeException(
            code=ErrorCode.INVALID_ARGUMENT,
            message=ExceptionsMessage.MetricTypeValueInvalid,
        )

    def _parse_value_for_text_sql(
        self, need_parse: bool, table, column_name: str, value
    ):
        if not need_parse:
            return value
        try:
            type_str = str(table.c[column_name].type)
            if type_str.startswith("VECTOR") and value is not None:
                return Vector._from_db(value)
            if type_str.startswith("JSON") and value is not None:
                return json.loads(value)
        except KeyError:
            return value
        return value

    def search(
        self,
        collection_name: str,
        data: Union[list, dict],
        anns_field: str,
        with_dist: bool = False,
        flter=None,
        limit: int = 10,
        output_fields: Optional[List[str]] = None,
        search_params: Optional[dict] = None,
        timeout: Optional[float] = None, # pylint: disable=unused-argument
        partition_names: Optional[List[str]] = None,
        **kwargs, # pylint: disable=unused-argument
    ) -> List[dict]:
        """Perform ann search.
        Note: OceanBase does not support batch search now. `data` & the return value is not a batch.
        
        Args:
            collection_name (string): collection name
            data (list): the vector/sparse_vector data to search
            anns_field (string): which vector field to search
            with_dist (bool): return result with distance
            flter: do ann search with filter (note: parameter name is intentionally 'flter' to distinguish it from the built-in function)
            limit (int): top K
            output_fields (Optional[List[str]]): output fields
            search_params (Optional[dict]): Only `metric_type` with value `l2`/`neg_ip` supported
            timeout (Optional[float]): not used in OceanBase
            partition_names (Optional[List[str]]): limit the query to certain partitions
            **kwargs: additional arguments

        Returns:
            List[dict]: A list of records, each record is a dict indicating a mapping from
                column_name to column value.
        """
        if not (isinstance(data, list) or isinstance(data, dict)):
            raise ValueError("'data' type must be in 'list'/'dict'")

        lower_metric_type_str = "l2"
        if search_params is not None:
            if "metric_type" in search_params:
                if not isinstance(search_params["metric_type"], str):
                    raise VectorMetricTypeException(
                        code=ErrorCode.INVALID_ARGUMENT,
                        message=ExceptionsMessage.MetricTypeParamTypeInvalid,
                    )
                lower_metric_type_str = search_params["metric_type"].lower()
                if lower_metric_type_str not in (
                    "l2", "neg_ip", "cosine", "ip"
                ):
                    raise VectorMetricTypeException(
                        code=ErrorCode.INVALID_ARGUMENT,
                        message=ExceptionsMessage.MetricTypeValueInvalid,
                    )
        distance_func = self._parse_metric_type_str_to_dist_func(lower_metric_type_str)

        try:
            table = Table(collection_name, self.metadata_obj, autoload_with=self.engine)
        except NoSuchTableError as e:
            raise CollectionStatusException(
                code=ErrorCode.COLLECTION_NOT_FOUND,
                message=ExceptionsMessage.CollectionNotExists,
            ) from e

        if output_fields is not None:
            columns = [table.c[column_name] for column_name in output_fields]
        else:
            columns = [table.c[column.name] for column in table.columns]

        if with_dist:
            if isinstance(data, list):
                columns.append(distance_func(table.c[anns_field],
                    "[" + ",".join([str(np.float32(v)) for v in data]) + "]"))
            else:
                columns.append(distance_func(table.c[anns_field], f"{data}"))
        stmt = select(*columns)

        if flter is not None:
            stmt = stmt.where(*flter)
        
        if isinstance(data, list):
            stmt = stmt.order_by(distance_func(table.c[anns_field],
                "[" + ",".join([str(np.float32(v)) for v in data]) + "]"))
        else:
            stmt = stmt.order_by(distance_func(table.c[anns_field], f"{data}"))
        stmt_str = (
            str(stmt.compile(
                dialect=self.engine.dialect,
                compile_kwargs={"literal_binds": True}
            ))
            + f" APPROXIMATE limit {limit}"
        )

        with self.engine.connect() as conn:
            with conn.begin():
                if partition_names is None:
                    execute_res = conn.execute(text(stmt_str))
                else:
                    stmt_str = self._insert_partition_hint_for_query_sql(
                        stmt_str, f"PARTITION({', '.join(partition_names)})"
                    )
                    logging.debug(stmt_str)
                    execute_res = conn.execute(text(stmt_str))
                data_res = execute_res.fetchall()
                columns = list(execute_res.keys())
                res = [
                    {
                        columns[i]: self._parse_value_for_text_sql(
                            True, table, columns[i], value
                        )
                        for i, value in enumerate(row)
                    }
                    for row in data_res
                ]
                if with_dist:
                    return sorted(res, key=lambda x: x[columns[-1]])
                return res

    def query(
        self,
        collection_name: str,
        flter=None,
        output_fields: Optional[List[str]] = None,
        timeout: Optional[float] = None, # pylint: disable=unused-argument
        partition_names: Optional[List[str]] = None,
        **kwargs, # pylint: disable=unused-argument
    ) -> List[dict]:
        """Query records.
        
        Args:
            collection_name (string): collection name
            flter: do ann search with filter (note: parameter name is intentionally 'flter' to distinguish it from the built-in function)
            output_fields (Optional[List[str]]): output fields
            timeout (Optional[float]): not used in OceanBase
            partition_names (Optional[List[str]]): limit the query to certain partitions
            **kwargs: additional arguments

        Returns:
            List[dict]: A list of records, each record is a dict indicating a mapping from
                column_name to column value.
        """
        try:
            table = Table(collection_name, self.metadata_obj, autoload_with=self.engine)
        except NoSuchTableError as e:
            raise CollectionStatusException(
                code=ErrorCode.COLLECTION_NOT_FOUND,
                message=ExceptionsMessage.CollectionNotExists,
            ) from e

        if output_fields is not None:
            columns = [table.c[column_name] for column_name in output_fields]
            stmt = select(*columns)
        else:
            stmt = select(table)

        if flter is not None:
            stmt = stmt.where(*flter)

        with self.engine.connect() as conn:
            with conn.begin():
                if partition_names is None:
                    execute_res = conn.execute(stmt)
                else:
                    stmt_str = str(stmt.compile(
                        dialect=self.engine.dialect,
                        compile_kwargs={"literal_binds": True}
                    ))
                    stmt_str = self._insert_partition_hint_for_query_sql(
                        stmt_str, f"PARTITION({', '.join(partition_names)})"
                    )
                    logging.debug(stmt_str)
                    execute_res = conn.execute(text(stmt_str))
                data_res = execute_res.fetchall()
                columns = list(execute_res.keys())
                return [
                    {
                        columns[i]: self._parse_value_for_text_sql(
                            partition_names is not None, table, columns[i], value
                        )
                        for i, value in enumerate(row)
                    }
                    for row in data_res
                ]

    def get(
        self,
        collection_name: str,
        ids: Union[list, str, int] = None,
        output_fields: Optional[List[str]] = None,
        timeout: Optional[float] = None, # pylint: disable=unused-argument
        partition_names: Optional[List[str]] = None,
        **kwargs, # pylint: disable=unused-argument
    ) -> List[dict]:
        """Get records with specified primary field `ids`.
        
        Args:
            collection_name (string): collection name
            ids (Union[list, str, int]): specified primary field values
            output_fields (Optional[List[str]]): output fields
            timeout (Optional[float]): not used in OceanBase
            partition_names (Optional[List[str]]): limit the query to certain partitions
            **kwargs: additional arguments

        Returns:
            List[dict]: A list of records, each record is a dict indicating a mapping from
                column_name to column value.
        """
        try:
            table = Table(collection_name, self.metadata_obj, autoload_with=self.engine)
        except NoSuchTableError as e:
            raise CollectionStatusException(
                code=ErrorCode.COLLECTION_NOT_FOUND,
                message=ExceptionsMessage.CollectionNotExists,
            ) from e

        if output_fields is not None:
            columns = [table.c[column_name] for column_name in output_fields]
            stmt = select(*columns)
        else:
            stmt = select(table)

        primary_keys = table.primary_key
        pkey_names = [column.name for column in primary_keys]
        if len(pkey_names) > 1:
            raise MilvusCompatibilityException(
                code=ErrorCode.INVALID_ARGUMENT,
                message=ExceptionsMessage.UsingInIDsWhenMultiPrimaryKey,
            )
        if isinstance(ids, list):
            where_in_clause = table.c[pkey_names[0]].in_(ids)
        elif isinstance(ids, (str, int)):
            where_in_clause = table.c[pkey_names[0]].in_([ids])
        else:
            raise TypeError("'ids' is not a list/str/int")

        stmt = stmt.where(where_in_clause)
        with self.engine.connect() as conn:
            with conn.begin():
                if partition_names is None:
                    execute_res = conn.execute(stmt)
                else:
                    stmt_str = str(stmt.compile(
                        dialect=self.engine.dialect,
                        compile_kwargs={"literal_binds": True}
                    ))
                    stmt_str = self._insert_partition_hint_for_query_sql(
                        stmt_str, f"PARTITION({', '.join(partition_names)})"
                    )
                    logging.debug(stmt_str)
                    execute_res = conn.execute(text(stmt_str))
                data_res = execute_res.fetchall()
                columns = list(execute_res.keys())
                return [
                    {
                        columns[i]: self._parse_value_for_text_sql(
                            partition_names is not None, table, columns[i], value
                        )
                        for i, value in enumerate(row)
                    }
                    for row in data_res
                ]

    def delete(
        self,
        collection_name: str,
        ids: Optional[Union[list, str, int]] = None,
        timeout: Optional[float] = None, # pylint: disable=unused-argument
        flter=None,
        partition_name: Optional[str] = "",
        **kwargs, # pylint: disable=unused-argument
    ) -> dict:
        """Delete data in collection.

        Args:
            collection_name (string): collection name
            ids (Optional[Union[list, str, int]]): a list of primary keys value
            timeout (Optional[float]): not used in OceanBase
            flter: delete with filter (note: parameter name is intentionally 'flter' to distinguish it from the built-in function)
            partition_name (Optional[str]): limit the query to certain partition
            **kwargs: additional arguments

        Returns:
            dict: deletion result
        """
        try:
            table = Table(collection_name, self.metadata_obj, autoload_with=self.engine)
        except NoSuchTableError as e:
            raise CollectionStatusException(
                code=ErrorCode.COLLECTION_NOT_FOUND,
                message=ExceptionsMessage.CollectionNotExists,
            ) from e

        where_in_clause = None
        if ids is not None:
            primary_keys = table.primary_key
            pkey_names = [column.name for column in primary_keys]
            if len(pkey_names) > 1:
                raise MilvusCompatibilityException(
                    code=ErrorCode.INVALID_ARGUMENT,
                    message=ExceptionsMessage.UsingInIDsWhenMultiPrimaryKey,
                )
            if isinstance(ids, list):
                where_in_clause = table.c[pkey_names[0]].in_(ids)
            elif isinstance(ids, (str, int)):
                where_in_clause = table.c[pkey_names[0]].in_([ids])
            else:
                raise TypeError("'ids' is not a list/str/int")

        with self.engine.connect() as conn:
            with conn.begin():
                delete_clause = (
                    delete(table).with_hint(f"PARTITION({partition_name})")
                    if partition_name is not None and partition_name != ""
                    else delete(table)
                )
                if where_in_clause is None and flter is None:
                    conn.execute(delete_clause)
                elif where_in_clause is not None and flter is None:
                    conn.execute(delete_clause.where(where_in_clause))
                elif where_in_clause is None and flter is not None:
                    conn.execute(delete_clause.where(*flter))
                else:
                    conn.execute(delete_clause.where(and_(where_in_clause, *flter)))

    def insert(
        self,
        collection_name: str,
        data: Union[Dict, List[Dict]],
        timeout: Optional[float] = None,
        partition_name: Optional[str] = "",
    ) -> (
        None
    ):  # pylint: disable=unused-argument
        """Insert data into collection.
        
        Args:
            collection_name (string): collection name
            data (Union[Dict, List[Dict]]): data that will be inserted
            timeout (Optional[float]): not used in OceanBase
            partition_name (Optional[str]): limit the query to certain partition
        """
        # different from milvus: OceanBase in mysql mode do not support returning.
        try:
            super().insert(
                table_name=collection_name, data=data, partition_name=partition_name
            )
        except NoSuchTableError as e:
            raise CollectionStatusException(
                code=ErrorCode.COLLECTION_NOT_FOUND,
                message=ExceptionsMessage.CollectionNotExists,
            ) from e

    def upsert(
        self,
        collection_name: str,
        data: Union[Dict, List[Dict]],
        timeout: Optional[float] = None, # pylint: disable=unused-argument
        partition_name: Optional[str] = "",
    ) -> List[Union[str, int]]:
        """Update data in table. If primary key is duplicated, replace it.
        
        Args:
            collection_name (string): collection name
            data (Union[Dict, List[Dict]]): data that will be upserted
            timeout (Optional[float]): not used in OceanBase
            partition_name (Optional[str]): limit the query to certain partition

        Returns:
            List[Union[str, int]]: list of primary keys
        """
        try:
            super().upsert(
                table_name=collection_name, data=data, partition_name=partition_name
            )
        except NoSuchTableError as e:
            raise CollectionStatusException(
                code=ErrorCode.COLLECTION_NOT_FOUND,
                message=ExceptionsMessage.CollectionNotExists,
            ) from e

    def perform_raw_text_sql(self, text_sql: str):
        return super().perform_raw_text_sql(text_sql)
