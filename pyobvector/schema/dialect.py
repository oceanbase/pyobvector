"""OceanBase dialect."""
from sqlalchemy import util
from sqlalchemy.dialects.mysql import aiomysql, pymysql

from .reflection import OceanBaseTableDefinitionParser
from .vector import VECTOR
from .sparse_vector import SPARSE_VECTOR
from .geo_srid_point import POINT


def _ensure_server_version_info(dialect, connection):
    """Ensure server_version_info is initialized to prevent TypeError.
    
    Args:
        dialect: The dialect instance
        connection: The connection object
    """
    if dialect.server_version_info is None:
        try:
            if hasattr(connection, 'connection') and hasattr(connection.connection, 'server_version'):
                version = connection.connection.server_version
                if version:
                    version_parts = version.split('.')[:3]
                    dialect.server_version_info = tuple(int(part) for part in version_parts)
                    return
            elif hasattr(connection, 'dialect') and hasattr(connection.dialect, '_get_server_version_info'):
                dialect.server_version_info = connection.dialect._get_server_version_info(connection)
                return
        except (AttributeError, ValueError, TypeError):
            pass
        dialect.server_version_info = (5, 7, 20)


class OceanBaseDialect(pymysql.MySQLDialect_pymysql):
    # not change dialect name, since it is a subclass of pymysql.MySQLDialect_pymysql
    # name = "oceanbase"
    """Ocenbase dialect."""
    supports_statement_cache = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ischema_names["VECTOR"] = VECTOR
        self.ischema_names["SPARSEVECTOR"] = SPARSE_VECTOR
        self.ischema_names["point"] = POINT

    def get_isolation_level(self, connection):
        """Override to handle None server_version_info.
        
        This method prevents TypeError when server_version_info is None
        during isolation level checks.
        """
        _ensure_server_version_info(self, connection)
        return super().get_isolation_level(connection)

    @util.memoized_property
    def _tabledef_parser(self):
        """return the MySQLTableDefinitionParser, generate if needed.

        The deferred creation ensures that the dialect has
        retrieved server version information first.

        """
        preparer = self.identifier_preparer
        default_schema = self.default_schema_name
        return OceanBaseTableDefinitionParser(
            self, preparer, default_schema=default_schema
        )


class AsyncOceanBaseDialect(aiomysql.MySQLDialect_aiomysql):
    """OceanBase async dialect."""
    supports_statement_cache = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ischema_names["VECTOR"] = VECTOR
        self.ischema_names["SPARSEVECTOR"] = SPARSE_VECTOR
        self.ischema_names["point"] = POINT

    def get_isolation_level(self, connection):
        """Override to handle None server_version_info.
        
        This method prevents TypeError when server_version_info is None
        during isolation level checks.
        """
        _ensure_server_version_info(self, connection)
        return super().get_isolation_level(connection)

    @util.memoized_property
    def _tabledef_parser(self):
        """return the MySQLTableDefinitionParser, generate if needed.

        The deferred creation ensures that the dialect has
        retrieved server version information first.

        """
        preparer = self.identifier_preparer
        default_schema = self.default_schema_name
        return OceanBaseTableDefinitionParser(
            self, preparer, default_schema=default_schema
        )
