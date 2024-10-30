from sqlalchemy import util
from sqlalchemy.dialects.mysql import aiomysql, pymysql

from .reflection import OceanBaseTableDefinitionParser
from .vector import VECTOR

class OceanBaseDialect(pymysql.MySQLDialect_pymysql):
    # not change dialect name, since it is a subclass of pymysql.MySQLDialect_pymysql
    # name = "oceanbase"
    supports_statement_cache = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ischema_names["VECTOR"] = VECTOR

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
    supports_statement_cache = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ischema_names["VECTOR"] = VECTOR

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
