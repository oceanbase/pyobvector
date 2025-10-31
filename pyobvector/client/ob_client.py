import logging
from typing import List, Optional, Dict, Union
from urllib.parse import quote

import sqlalchemy.sql.functions as func_mod
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
from sqlalchemy.dialects import registry
from sqlalchemy.exc import NoSuchTableError

from .index_param import IndexParams
from .partitions import ObPartition
from ..schema import (
    ObTable,
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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ObClient:
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

        setattr(func_mod, "l2_distance", l2_distance)
        setattr(func_mod, "cosine_distance", cosine_distance)
        setattr(func_mod, "inner_product", inner_product)
        setattr(func_mod, "negative_inner_product", negative_inner_product)
        setattr(func_mod, "ST_GeomFromText", ST_GeomFromText)
        setattr(func_mod, "st_distance", st_distance)
        setattr(func_mod, "st_dwithin", st_dwithin)
        setattr(func_mod, "st_astext", st_astext)

        user = quote(user, safe="")
        password = quote(password, safe="")

        connection_str = (
            f"mysql+oceanbase://{user}:{password}@{uri}/{db_name}?charset=utf8mb4"
        )
        self.engine = create_engine(connection_str, **kwargs)
        self.metadata_obj = MetaData()
        self.metadata_obj.reflect(bind=self.engine)

        with self.engine.connect() as conn:
            with conn.begin():
                res = conn.execute(text("SELECT OB_VERSION() FROM DUAL"))
                version = [r[0] for r in res][0]
                self.ob_version = ObVersion.from_db_version_string(version)

    def refresh_metadata(self, tables: Optional[list[str]] = None):
        """Reload metadata from the database.

        Args:
            tables (Optional[list[str]]): names of the tables to refresh. If None, refresh all tables.
        """
        if tables is not None:
            for table_name in tables:
                if table_name in self.metadata_obj.tables:
                    self.metadata_obj.remove(Table(table_name, self.metadata_obj))
            self.metadata_obj.reflect(bind=self.engine, only=tables, extend_existing=True)
        else:
            self.metadata_obj.clear()
            self.metadata_obj.reflect(bind=self.engine, extend_existing=True)

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
        """Check if table exists.

        Args:
            table_name (string): table name

        Returns:
            bool: True if table exists, False otherwise
        """
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def create_table(
        self,
        table_name: str,
        columns: List[Column],
        indexes: Optional[List[Index]] = None,
        partitions: Optional[ObPartition] = None,
        **kwargs,
    ):
        """Create a table.

        Args:
            table_name (string): table name
            columns (List[Column]): column schema
            indexes (Optional[List[Index]]): optional index schema
            partitions (Optional[ObPartition]): optional partition strategy
            **kwargs: additional keyword arguments
        """
        kwargs.setdefault("extend_existing", True)
        with self.engine.connect() as conn:
            with conn.begin():
                if indexes is not None:
                    table = ObTable(
                        table_name,
                        self.metadata_obj,
                        *columns,
                        *indexes,
                        **kwargs,
                    )
                else:
                    table = ObTable(
                        table_name,
                        self.metadata_obj,
                        *columns,
                        **kwargs,
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

    def insert(
        self,
        table_name: str,
        data: Union[Dict, List[Dict]],
        partition_name: Optional[str] = "",
    ):
        """Insert data into table.

        Args:
            table_name (string): table name
            data (Union[Dict, List[Dict]]): data that will be inserted
            partition_name (Optional[str]): limit the query to certain partition
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
            table_name (string): table name
            data (Union[Dict, List[Dict]]): data that will be upserted
            partition_name (Optional[str]): limit the query to certain partition
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
            table_name (string): table name
            values_clause: update values clause
            where_clause: update with filter
            partition_name (Optional[str]): limit the query to certain partition

        Example:
            ... code-block:: python

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
            table_name (string): table name
            ids (Optional[Union[list, str, int]]): ids of data to delete
            where_clause: delete with filter
            partition_name (Optional[str]): limit the query to certain partition
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
        ids: Optional[Union[list, str, int]] = None,
        where_clause=None,
        output_column_name: Optional[List[str]] = None,
        partition_names: Optional[List[str]] = None,
        n_limits: Optional[int] = None,
    ):
        """Get records with specified primary field `ids`.

        Args:
            table_name (string): table name
            ids (Optional[Union[list, str, int]]): specified primary field values
            where_clause: SQL filter
            output_column_name (Optional[List[str]]): output fields name
            partition_names (Optional[List[str]]): limit the query to certain partitions
            n_limits (Optional[int]): limit the number of results

        Returns:
            Result object from SQLAlchemy execution
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

        if n_limits is not None:
            stmt = stmt.limit(n_limits)

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
                return execute_res

    def perform_raw_text_sql(
        self,
        text_sql: str,
    ):
        """Execute raw text SQL."""
        with self.engine.connect() as conn:
            with conn.begin():
                return conn.execute(text(text_sql))

    def add_columns(
        self,
        table_name: str,
        columns: list[Column],
    ):
        """Add multiple columns to an existing table.

        Args:
            table_name (string): table name
            columns (list[Column]): list of SQLAlchemy Column objects representing the new columns
        """
        compiler = self.engine.dialect.ddl_compiler(self.engine.dialect, None)
        column_specs = [compiler.get_column_specification(column) for column in columns]
        columns_ddl = ", ".join(f"ADD COLUMN {spec}" for spec in column_specs)

        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"ALTER TABLE `{table_name}` {columns_ddl}")
                )

        self.refresh_metadata([table_name])

    def drop_columns(
        self,
        table_name: str,
        column_names: list[str],
    ):
        """Drop multiple columns from an existing table.

        Args:
            table_name (string): table name
            column_names (list[str]): names of the columns to drop
        """
        columns_ddl = ", ".join(f"DROP COLUMN `{name}`" for name in column_names)

        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text(f"ALTER TABLE `{table_name}` {columns_ddl}")
                )

        self.refresh_metadata([table_name])
