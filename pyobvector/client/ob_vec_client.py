"""OceanBase Vector Store Client."""
import logging
from typing import List, Optional, Union

import numpy as np
from sqlalchemy import (
    Table,
    Column,
    Index,
    select,
    text,
)
from sqlalchemy.schema import CreateTable

from .exceptions import ClusterVersionException, ErrorCode, ExceptionsMessage
from .fts_index_param import FtsIndexParam
from .index_param import IndexParams, IndexParam
from .ob_client import ObClient
from .partitions import ObPartition
from ..schema import (
    ObTable,
    VectorIndex,
    FtsIndex,
)
from ..util import ObVersion

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ObVecClient(ObClient):
    """The OceanBase Vector Client"""

    def __init__(
        self,
        uri: str = "127.0.0.1:2881",
        user: str = "root@test",
        password: str = "",
        db_name: str = "test",
        **kwargs,
    ):
        super().__init__(uri, user, password, db_name, **kwargs)

        if self.ob_version < ObVersion.from_db_version_nums(4, 3, 3, 0):
            raise ClusterVersionException(
                code=ErrorCode.NOT_SUPPORTED,
                message=ExceptionsMessage.ClusterVersionIsLow % ("Vector Store", "4.3.3.0"),
            )

    def _get_sparse_vector_index_params(
        self, vidxs: Optional[IndexParams]
    ):
        if vidxs is None:
            return None
        return [
            vidx for vidx in vidxs
            if vidx.is_index_type_sparse_vector()
        ]

    def create_table_with_index_params(
        self,
        table_name: str,
        columns: List[Column],
        indexes: Optional[List[Index]] = None,
        vidxs: Optional[IndexParams] = None,
        fts_idxs: Optional[List[FtsIndexParam]] = None,
        partitions: Optional[ObPartition] = None,
    ):
        """Create table with optional index_params.

        Args:
            table_name (string): table name
            columns (List[Column]): column schema
            indexes (Optional[List[Index]]): optional common index schema
            vidxs (Optional[IndexParams]): optional vector index schema
            fts_idxs (Optional[List[FtsIndexParam]]): optional full-text search index schema
            partitions (Optional[ObPartition]): optional partition strategy
        """
        sparse_vidxs = self._get_sparse_vector_index_params(vidxs)
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
                if sparse_vidxs is not None and len(sparse_vidxs) > 0:
                    create_table_sql = str(CreateTable(table).compile(self.engine))
                    new_sql = create_table_sql[:create_table_sql.rfind(')')]
                    for sparse_vidx in sparse_vidxs:
                        new_sql += f",\n\tVECTOR INDEX {sparse_vidx.index_name}({sparse_vidx.field_name}) with (distance=inner_product)"
                    new_sql += "\n)"
                    conn.execute(text(new_sql))
                else:
                    table.create(self.engine, checkfirst=True)
                # do partition
                if partitions is not None:
                    conn.execute(
                        text(f"ALTER TABLE `{table_name}` {partitions.do_compile()}")
                    )
                # create vector indexes
                if vidxs is not None:
                    for vidx in vidxs:
                        if vidx.is_index_type_sparse_vector():
                            continue
                        vidx = VectorIndex(
                            vidx.index_name,
                            table.c[vidx.field_name],
                            params=vidx.param_str(),
                        )
                        vidx.create(self.engine, checkfirst=True)
                # create fts indexes
                if fts_idxs is not None:
                    for fts_idx in fts_idxs:
                        idx_cols = [table.c[field_name] for field_name in fts_idx.field_names]
                        fts_idx = FtsIndex(
                            fts_idx.index_name,
                            fts_idx.param_str(),
                            *idx_cols,
                        )
                        fts_idx.create(self.engine, checkfirst=True)

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
            table_name (string): table name
            is_vec_index (bool): common index or vector index
            index_name (string): index name
            column_names (List[string]): create index on which columns
            vidx_params (Optional[str]): vector index params, for example 'distance=l2, type=hnsw, lib=vsag'
            **kw: additional keyword arguments
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
            table_name (string): table name
            vidx_param (IndexParam): vector index parameter
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

    def create_fts_idx_with_fts_index_param(
        self,
        table_name: str,
        fts_idx_param: FtsIndexParam,
    ):
        """Create fts index with fts index parameter.
        
        Args:
            table_name (string): table name
            fts_idx_param (FtsIndexParam): fts index parameter
        """
        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)
        with self.engine.connect() as conn:
            with conn.begin():
                idx_cols = [table.c[field_name] for field_name in fts_idx_param.field_names]
                fts_idx = FtsIndex(
                    fts_idx_param.index_name,
                    fts_idx_param.param_str(),
                    *idx_cols,
                )
                fts_idx.create(self.engine, checkfirst=True)

    def refresh_index(
        self,
        table_name: str,
        index_name: str,
        trigger_threshold: int = 10000,
    ):
        """Refresh vector index for performance.

        Args:
            table_name (string): table name
            index_name (string): vector index name
            trigger_threshold (int): If delta_buffer_table row count is greater than `trigger_threshold`,
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
            table_name (string): table name
            index_name (string): vector index name
            trigger_threshold (float): threshold value for rebuilding index
        """
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(
                        f"CALL DBMS_VECTOR.REBUILD_INDEX('{index_name}', "
                        f"'{table_name}', '', {trigger_threshold})"
                    )
                )

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
        vec_data: Union[list, dict],
        vec_column_name: str,
        distance_func,
        with_dist: bool = False,
        topk: int = 10,
        output_column_names: Optional[List[str]] = None,
        output_columns: Optional[Union[List, tuple]] = None,
        extra_output_cols: Optional[List] = None,
        where_clause=None,
        partition_names: Optional[List[str]] = None,
        idx_name_hint: Optional[List[str]] = None,
        distance_threshold: Optional[float] = None,
        **kwargs,
    ):  # pylint: disable=unused-argument
        """Perform ann search.

        Args:
            table_name (string): table name
            vec_data (Union[list, dict]): the vector/sparse_vector data to search
            vec_column_name (string): which vector field to search
            distance_func: function to calculate distance between vectors
            with_dist (bool): return result with distance
            topk (int): top K
            output_column_names (Optional[List[str]]): output fields
            output_columns (Optional[Union[List, tuple]]): output columns as SQLAlchemy Column objects
                or expressions. Similar to SQLAlchemy's select() function arguments.
                If provided, takes precedence over output_column_names.
            extra_output_cols (Optional[List]): additional output columns
            where_clause: do ann search with filter
            partition_names (Optional[List[str]]): limit the query to certain partitions
            idx_name_hint (Optional[List[str]]): post-filtering enabled if vector index name is specified
                Or pre-filtering enabled
            distance_threshold (Optional[float]): filter results where distance <= threshold.
            **kwargs: additional arguments
        """
        if not (isinstance(vec_data, list) or isinstance(vec_data, dict)):
            raise ValueError("'vec_data' type must be in 'list'/'dict'")

        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)

        columns = []
        if output_columns:
            if isinstance(output_columns, (list, tuple)):
                columns = list(output_columns)
            else:
                columns = [output_columns]
        elif output_column_names:
            columns = [table.c[column_name] for column_name in output_column_names]
        else:
            columns = [table.c[column.name] for column in table.columns]

        if extra_output_cols:
            columns.extend(extra_output_cols)

        if with_dist:
            if isinstance(vec_data, list):
                columns.append(
                    distance_func(
                        table.c[vec_column_name],
                        "[" + ",".join([str(np.float32(v)) for v in vec_data]) + "]",
                    )
                )
            else:
                columns.append(
                    distance_func(
                        table.c[vec_column_name], f"{vec_data}"
                    )
                )
        # if idx_name_hint is not None:
        #     stmt = select(*columns).with_hint(
        #         table,
        #         f"index(%(name)s {idx_name_hint})",
        #         "oracle"
        #     )
        # else:
        stmt = select(*columns)

        if where_clause is not None:
            stmt = stmt.where(*where_clause)

        # Add distance threshold filter in SQL WHERE clause
        if distance_threshold is not None:
            if isinstance(vec_data, list):
                dist_expr = distance_func(
                    table.c[vec_column_name],
                    "[" + ",".join([str(np.float32(v)) for v in vec_data]) + "]",
                )
            else:
                dist_expr = distance_func(
                    table.c[vec_column_name], f"{vec_data}"
                )
            stmt = stmt.where(dist_expr <= distance_threshold)

        if isinstance(vec_data, list):
            stmt = stmt.order_by(
                distance_func(
                    table.c[vec_column_name],
                    "[" + ",".join([str(np.float32(v)) for v in vec_data]) + "]",
                )
            )
        else:
            stmt = stmt.order_by(
                distance_func(
                    table.c[vec_column_name], f"{vec_data}"
                )
            )
        stmt_str = (
            str(stmt.compile(
                dialect=self.engine.dialect,
                compile_kwargs={"literal_binds": True}
            ))
            + f" APPROXIMATE limit {topk}"
        )
        with self.engine.connect() as conn:
            with conn.begin():
                if idx_name_hint is not None:
                    idx = stmt_str.find("SELECT ")
                    stmt_str = f"SELECT /*+ index({table_name} {idx_name_hint}) */ " + stmt_str[idx + len("SELECT "):]

                if partition_names is None:
                    return conn.execute(text(stmt_str))
                stmt_str = self._insert_partition_hint_for_query_sql(
                    stmt_str, f"PARTITION({', '.join(partition_names)})"
                )
                return conn.execute(text(stmt_str))

    def post_ann_search(
        self,
        table_name: str,
        vec_data: list,
        vec_column_name: str,
        distance_func,
        with_dist: bool = False,
        topk: int = 10,
        output_column_names: Optional[List[str]] = None,
        extra_output_cols: Optional[List] = None,
        where_clause=None,
        partition_names: Optional[List[str]] = None,
        str_list: Optional[List[str]] = None,
        **kwargs,
    ):  # pylint: disable=unused-argument
        """Perform post ann search.

        Args:
            table_name (string): table name
            vec_data (list): the vector data to search
            vec_column_name (string): which vector field to search
            distance_func: function to calculate distance between vectors
            with_dist (bool): return result with distance
            topk (int): top K
            output_column_names (Optional[List[str]]): output fields
            extra_output_cols (Optional[List]): additional output columns
            where_clause: do ann search with filter
            partition_names (Optional[List[str]]): limit the query to certain partitions
            str_list (Optional[List[str]]): list to append SQL string to
            **kwargs: additional arguments
        """
        table = Table(table_name, self.metadata_obj, autoload_with=self.engine)

        columns = []
        if output_column_names is not None:
            columns.extend([table.c[column_name] for column_name in output_column_names])
        else:
            columns.extend([table.c[column.name] for column in table.columns])
        if extra_output_cols is not None:
            columns.extend(extra_output_cols)

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
        ).limit(topk)

        with self.engine.connect() as conn:
            with conn.begin():
                if partition_names is None:
                    if str_list is not None:
                        str_list.append(
                            str(stmt.compile(
                                dialect=self.engine.dialect,
                                compile_kwargs={"literal_binds": True}
                            ))
                        )
                    return conn.execute(stmt)
                stmt_str = str(stmt.compile(
                    dialect=self.engine.dialect,
                    compile_kwargs={"literal_binds": True}
                ))
                stmt_str = self._insert_partition_hint_for_query_sql(
                    stmt_str, f"PARTITION({', '.join(partition_names)})"
                )
                if str_list is not None:
                    str_list.append(stmt_str)
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
        **kwargs,
    ):  # pylint: disable=unused-argument
        """Perform precise vector search.

        Args:
            table_name (string): table name
            vec_data (list): the vector data to search
            vec_column_name (string): which vector field to search
            distance_func: function to calculate distance between vectors
            topk (int): top K
            output_column_names (Optional[List[str]]): output column names
            where_clause: do ann search with filter
            **kwargs: additional arguments
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
