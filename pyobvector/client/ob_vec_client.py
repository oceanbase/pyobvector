"""OceanBase Vector Store Client."""

import logging
from typing import List, Optional, Dict, Union
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Index,
    select,
    delete,
    update,
    insert,
    text,
    inspect,
    and_,
)
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.dialects import registry
import sqlalchemy.sql.functions as func_mod
import numpy as np
from .index_param import IndexParams, IndexParam
from ..schema import (
    ObTable,
    VectorIndex,
    l2_distance,
    cosine_distance,
    inner_product,
    negative_inner_product,
    ST_GeomFromText,
    st_distance,
    st_dwithin,
    st_astext,
    ReplaceStmt,
)
from ..util import ObVersion
from .partitions import *
from .exceptions import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ObVecClient:
    """The OceanBase Client"""

    def __init__(
        self,
        uri: str = "127.0.0.1:2881",
        user: str = "root@test",
        password: str = "",
        db_name: str = "test",
        **kwargs,
    ):
        registry.register("mysql.oceanbase", "pyobvector.schema.dialect", "OceanBaseDialect")

        # ischema_names["VECTOR"] = VECTOR
        setattr(func_mod, "l2_distance", l2_distance)
        setattr(func_mod, "cosine_distance", cosine_distance)
        setattr(func_mod, "inner_product", inner_product)
        setattr(func_mod, "negative_inner_product", negative_inner_product)
        setattr(func_mod, "ST_GeomFromText", ST_GeomFromText)
        setattr(func_mod, "st_distance", st_distance)
        setattr(func_mod, "st_dwithin", st_dwithin)
        setattr(func_mod, "st_astext", st_astext)

        connection_str = (
            f"mysql+oceanbase://{user}:{password}@{uri}/{db_name}?charset=utf8mb4"
        )
        self.engine = create_engine(connection_str, **kwargs)
        self.metadata_obj = MetaData()

        with self.engine.connect() as conn:
            with conn.begin():
                res = conn.execute(text("SELECT OB_VERSION() FROM DUAL"))
                version = [r[0] for r in res][0]
                ob_version = ObVersion.from_db_version_string(version)
                if ob_version < ObVersion.from_db_version_nums(4, 3, 3, 0):
                    raise ClusterVersionException(
                        code=ErrorCode.NOT_SUPPORTED,
                        message=ExceptionsMessage.ClusterVersionIsLow,
                    )

    def _insert_partition_hint_for_query_sql(self, sql: str, partition_hint: str):
        from_index = sql.find("FROM")
        assert from_index != -1
        first_space_after_from = sql.find(" ", from_index + len("FROM") + 1)
        if first_space_after_from == -1:
            return sql + " " + partition_hint
        return (
            sql[:first_space_after_from]
            + " "
            + partition_hint
            + sql[first_space_after_from:]
        )

    def check_table_exists(self, table_name: str):
        """check if table exists.

        Args:
            table_name (string) : table name
        """
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def create_table(
        self,
        table_name: str,
        columns: List[Column],
        indexes: Optional[List[Index]] = None,
        partitions: Optional[ObPartition] = None,
    ):
        """Create a table.

        Args:
            table_name (string) : table name
            columns (List[Column]) : column schema
            indexes (Optional[List[Index]]) : optional index schema
            partitions (Optional[ObPartition]) : optional partition strategy
        """
        with self.engine.connect() as conn:
            with conn.begin():
                if indexes is not None:
                    table = ObTable(
                        table_name,
                        self.metadata_obj,
                        *columns,
                        *indexes,
                        extend_existing=True,
                    )
                else:
                    table = ObTable(
                        table_name,
                        self.metadata_obj,
                        *columns,
                        extend_existing=True,
                    )
                table.create(self.engine, checkfirst=True)
                # do partition
                if partitions is not None:
                    conn.execute(
                        text(f"ALTER TABLE `{table_name}` {partitions.do_compile()}")
                    )

    @classmethod
    def prepare_index_params(cls):
        """Create `IndexParams` to hold index configuration."""
        return IndexParams()

    def create_table_with_index_params(
        self,
        table_name: str,
        columns: List[Column],
        indexes: Optional[List[Index]] = None,
        vidxs: Optional[IndexParams] = None,
        partitions: Optional[ObPartition] = None,
    ):
        """Create table with optional index_params.

        Args:
            table_name (string) : table name
            columns (List[Column]) : column schema
            indexes (Optional[List[Index]]) : optional common index schema
            vids (Optional[IndexParams]) : optional vector index schema
            partitions (Optional[ObPartition]) : optional partition strategy
        """
        with self.engine.connect() as conn:
            with conn.begin():
                # create table with common index
                if indexes is not None:
                    table = ObTable(
                        table_name,
                        self.metadata_obj,
                        *columns,
                        *indexes,
                        extend_existing=True,
                    )
                else:
                    table = ObTable(
                        table_name,
                        self.metadata_obj,
                        *columns,
                        extend_existing=True,
                    )
                table.create(self.engine, checkfirst=True)
                # do partition
                if partitions is not None:
                    conn.execute(
                        text(f"ALTER TABLE `{table_name}` {partitions.do_compile()}")
                    )
                # create vector indexes
                if vidxs is not None:
                    for vidx in vidxs:
                        vidx = VectorIndex(
                            vidx.index_name,
                            table.c[vidx.field_name],
                            params=vidx.param_str(),
                        )
                        vidx.create(self.engine, checkfirst=True)

    def create_index(
        self,
        table_name: str,
        is_vec_index: bool,
        index_name: str,
        column_names: List[str],
        vidx_params: Optional[str] = None,
        **kw,
    ):
        """Create common index or vector index.

        Args:
            table_name (string) : table name
            is_vec_index (bool) : common index or vector index
            index_name (string) : index name
            column_names (List[string]) : create index on which columns
            vidx_params (Optional[str]) :
                vector index params, for example 'distance=l2, type=hnsw, lib=vsag'
        """
        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)
        columns = [table.c[column_name] for column_name in column_names]
        with self.engine.connect() as conn:
            with conn.begin():
                if is_vec_index:
                    vidx = VectorIndex(index_name, *columns, params=vidx_params, **kw)
                    vidx.create(self.engine, checkfirst=True)
                else:
                    idx = Index(index_name, *columns, **kw)
                    idx.create(self.engine, checkfirst=True)

    def create_vidx_with_vec_index_param(
        self,
        table_name: str,
        vidx_param: IndexParam,
    ):
        """Create vector index with vector index parameter.

        Args:
            table_name (string) : table name
            vidx_param (IndexParam) : vector index parameter
        """
        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)
        with self.engine.connect() as conn:
            with conn.begin():
                vidx = VectorIndex(
                    vidx_param.index_name,
                    table.c[vidx_param.field_name],
                    params=vidx_param.param_str(),
                )
                vidx.create(self.engine, checkfirst=True)

    def drop_table_if_exist(self, table_name: str):
        """Drop table if exists."""
        try:
            table = Table(table_name, self.metadata_obj, autoload_with=self.engine)
        except NoSuchTableError:
            return
        with self.engine.connect() as conn:
            with conn.begin():
                table.drop(self.engine, checkfirst=True)
                self.metadata_obj.remove(table)

    def drop_index(self, table_name: str, index_name: str):
        """drop index on specified table.

        If the index not exists, SQL ERROR 1091 will raise.
        """
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"DROP INDEX `{index_name}` ON `{table_name}`"))

    def refresh_index(
        self,
        table_name: str,
        index_name: str,
        trigger_threshold: int = 10000,
    ):
        """Refresh vector index for performance.

        Args:
        :param table_name (string) : table name
        :param index_name (string) : vector index name
        :param trigger_threshold (int) :
                If delta_buffer_table row count is greater than `trigger_threshold`,
                refreshing is actually triggered.
        """
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(
                        f"CALL DBMS_VECTOR.REFRESH_INDEX('{index_name}', "
                        f"'{table_name}', '', {trigger_threshold})"
                    )
                )

    def rebuild_index(
        self,
        table_name: str,
        index_name: str,
        trigger_threshold: float = 0.2,
    ):
        """Rebuild vector index for performance.

        Args:
        :param table_name (string) : table name
        :param index_name (string) : vector index name
        :param trigger_threshold (float)
        """
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(
                        f"CALL DBMS_VECTOR.REBUILD_INDEX('{index_name}', "
                        f"'{table_name}', '', {trigger_threshold})"
                    )
                )

    def insert(
        self,
        table_name: str,
        data: Union[Dict, List[Dict]],
        partition_name: Optional[str] = "",
    ):
        """Insert data into table.

        Args:
            table_name (string) : table name
            data (Union[Dict, List[Dict]]) : data that will be inserted
            partition_names (Optional[str]) : limit the query to certain partition
        """
        if isinstance(data, Dict):
            data = [data]

        if len(data) == 0:
            return

        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)

        with self.engine.connect() as conn:
            with conn.begin():
                if partition_name is None or partition_name == "":
                    conn.execute(insert(table).values(data))
                else:
                    conn.execute(
                        insert(table)
                        .with_hint(f"PARTITION({partition_name})")
                        .values(data)
                    )

    def upsert(
        self,
        table_name: str,
        data: Union[Dict, List[Dict]],
        partition_name: Optional[str] = "",
    ):
        """Update data in table. If primary key is duplicated, replace it.

        Args:
            table_name (string) : table name
            data (Union[Dict, List[Dict]]) : data that will be upserted
            partition_names (Optional[str]) : limit the query to certain partition
        """
        if isinstance(data, Dict):
            data = [data]

        if len(data) == 0:
            return

        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)

        with self.engine.connect() as conn:
            with conn.begin():
                upsert_stmt = (
                    ReplaceStmt(table).with_hint(f"PARTITION({partition_name})")
                    if partition_name is not None and partition_name != ""
                    else ReplaceStmt(table)
                )
                upsert_stmt = upsert_stmt.values(data)
                conn.execute(upsert_stmt)

    def update(
        self,
        table_name: str,
        values_clause,
        where_clause=None,
        partition_name: Optional[str] = "",
    ):
        """Update data in table.

        Args:
            table_name (string) : table name
            values_clause: update values clause
            where_clause: update with filter
            partition_name (Optional[str]) : limit the query to certain partition

        Example:
            .. code-block:: python

            data = [
                {"id": 112, "embedding": [1, 2, 3], "meta": {'doc':'hhh1'}},
                {"id": 190, "embedding": [0.13, 0.123, 1.213], "meta": {'doc':'hhh2'}},
            ]
            client.insert(collection_name=test_collection_name, data=data)
            client.update(
                table_name=test_collection_name,
                values_clause=[{'meta':{'doc':'HHH'}}],
                where_clause=[text("id=112")]
            )
        """
        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)

        with self.engine.connect() as conn:
            with conn.begin():
                update_stmt = (
                    update(table).with_hint(f"PARTITION({partition_name})")
                    if partition_name is not None and partition_name != ""
                    else update(table)
                )
                if where_clause is not None:
                    update_stmt = update_stmt.where(*where_clause).values(
                        *values_clause
                    )
                else:
                    update_stmt = update_stmt.values(*values_clause)
                conn.execute(update_stmt)

    def delete(
        self,
        table_name: str,
        ids: Optional[Union[list, str, int]] = None,
        where_clause=None,
        partition_name: Optional[str] = "",
    ):
        """Delete data in table.

        Args:
            table_name (string) : table name
            where_clause : delete with filter
            partition_names (Optional[str]) : limit the query to certain partition
        """
        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)
        where_in_clause = None
        if ids is not None:
            primary_keys = table.primary_key
            pkey_names = [column.name for column in primary_keys]
            if len(pkey_names) == 1:
                if isinstance(ids, list):
                    where_in_clause = table.c[pkey_names[0]].in_(ids)
                elif isinstance(ids, (str, int)):
                    where_in_clause = table.c[pkey_names[0]].in_([ids])
                else:
                    raise TypeError("'ids' is not a list/str/int")

        with self.engine.connect() as conn:
            with conn.begin():
                delete_stmt = (
                    delete(table).with_hint(f"PARTITION({partition_name})")
                    if partition_name is not None and partition_name != ""
                    else delete(table)
                )
                if where_in_clause is None and where_clause is None:
                    conn.execute(delete_stmt)
                elif where_in_clause is not None and where_clause is None:
                    conn.execute(delete_stmt.where(where_in_clause))
                elif where_in_clause is None and where_clause is not None:
                    conn.execute(delete_stmt.where(*where_clause))
                else:
                    conn.execute(
                        delete_stmt.where(and_(where_in_clause, *where_clause))
                    )

    def get(
        self,
        table_name: str,
        ids: Optional[Union[list, str, int]],
        where_clause = None,
        output_column_name: Optional[List[str]] = None,
        partition_names: Optional[List[str]] = None,
    ):
        """get records with specified primary field `ids`.

        Args:
        :param table_name (string) : table name
        :param ids : specified primary field values
        :param where_clause : SQL filter
        :param output_column_name (Optional[List[str]]) : output fields name
        :param partition_names (List[str]) : limit the query to certain partitions
        """
        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)
        if output_column_name is not None:
            columns = [table.c[column_name] for column_name in output_column_name]
            stmt = select(*columns)
        else:
            stmt = select(table)
        primary_keys = table.primary_key
        pkey_names = [column.name for column in primary_keys]
        where_in_clause = None
        if ids is not None and len(pkey_names) == 1:
            if isinstance(ids, list):
                where_in_clause = table.c[pkey_names[0]].in_(ids)
            elif isinstance(ids, (str, int)):
                where_in_clause = table.c[pkey_names[0]].in_([ids])
            else:
                raise TypeError("'ids' is not a list/str/int")

        if where_in_clause is not None and where_clause is None:
            stmt = stmt.where(where_in_clause)
        elif where_in_clause is None and where_clause is not None:
            stmt = stmt.where(*where_clause)
        elif where_in_clause is not None and where_clause is not None:
            stmt = stmt.where(and_(where_in_clause, *where_clause))

        with self.engine.connect() as conn:
            with conn.begin():
                if partition_names is None:
                    execute_res = conn.execute(stmt)
                else:
                    stmt_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))
                    stmt_str = self._insert_partition_hint_for_query_sql(
                        stmt_str, f"PARTITION({', '.join(partition_names)})"
                    )
                    logging.debug(stmt_str)
                    execute_res = conn.execute(text(stmt_str))
                return execute_res

    def set_ob_hnsw_ef_search(self, ob_hnsw_ef_search: int):
        """Set ob_hnsw_ef_search system variable."""
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"SET @@ob_hnsw_ef_search = {ob_hnsw_ef_search}"))

    def get_ob_hnsw_ef_search(self) -> int:
        """Get ob_hnsw_ef_search system variable."""
        with self.engine.connect() as conn:
            with conn.begin():
                res = conn.execute(text("show variables like 'ob_hnsw_ef_search'"))
                return int(res.fetchall()[0][1])

    def ann_search(
        self,
        table_name: str,
        vec_data: list,
        vec_column_name: str,
        distance_func,
        with_dist: bool = False,
        topk: int = 10,
        output_column_names: Optional[List[str]] = None,
        where_clause=None,
        partition_names: Optional[List[str]] = None,
    ):
        """perform ann search.

        Args:
            table_name (string) : table name
            vec_data (list) : the vector data to search
            vec_column_name (string) : which vector field to search
            distance_func : function to calculate distance between vectors
            with_dist (bool) : return result with distance
            topk (int) : top K
            output_column_names (Optional[List[str]]) : output fields
            where_clause : do ann search with filter
        """
        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)

        if output_column_names is not None:
            columns = [table.c[column_name] for column_name in output_column_names]
        else:
            columns = [table.c[column.name] for column in table.columns]

        if with_dist:
            columns.append(
                distance_func(
                    table.c[vec_column_name],
                    "[" + ",".join([str(np.float32(v)) for v in vec_data]) + "]",
                )
            )
        stmt = select(*columns)

        if where_clause is not None:
            stmt = stmt.where(*where_clause)

        stmt = stmt.order_by(
            distance_func(
                table.c[vec_column_name],
                "[" + ",".join([str(np.float32(v)) for v in vec_data]) + "]",
            )
        )
        stmt_str = (
            str(stmt.compile(compile_kwargs={"literal_binds": True}))
            + f" APPROXIMATE limit {topk}"
        )
        with self.engine.connect() as conn:
            with conn.begin():
                if partition_names is None:
                    return conn.execute(text(stmt_str))
                stmt_str = self._insert_partition_hint_for_query_sql(
                    stmt_str, f"PARTITION({', '.join(partition_names)})"
                )
                return conn.execute(text(stmt_str))

    def precise_search(
        self,
        table_name: str,
        vec_data: list,
        vec_column_name: str,
        distance_func,
        topk: int = 10,
        output_column_names: Optional[List[str]] = None,
        where_clause=None,
    ):
        """perform precise vector search.

        Args:
            table_name (string) : table name
            vec_data (list) : the vector data to search
            vec_column_name (string) : which vector field to search
            distance_func : function to calculate distance between vectors
            topk (int) : top K
            output_column_names (Optional[List[str]]) : output column names
            where_clause : do ann search with filter
        """
        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)

        if output_column_names is not None:
            columns = [table.c[column_name] for column_name in output_column_names]
            stmt = (
                select(*columns)
                .order_by(distance_func(table.c[vec_column_name], str(vec_data)))
                .limit(topk)
            )
            if where_clause is not None:
                stmt = stmt.where(*where_clause)
            with self.engine.connect() as conn:
                with conn.begin():
                    return conn.execute(stmt)
        else:
            stmt = (
                select(table)
                .order_by(distance_func(table.c[vec_column_name], str(vec_data)))
                .limit(topk)
            )
            if where_clause is not None:
                stmt = stmt.where(*where_clause)
            with self.engine.connect() as conn:
                with conn.begin():
                    return conn.execute(stmt)

    def perform_raw_text_sql(
        self,
        text_sql: str,
    ):
        """Execute raw text SQL."""
        with self.engine.connect() as conn:
            with conn.begin():
                return conn.execute(text(text_sql))
