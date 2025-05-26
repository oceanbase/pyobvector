import inspect

from sqlglot import parse_one as _parse_one


def parse_one(sql: str, dialect = "oceanbase"):
    signature = inspect.signature(_parse_one)
    if 'dialect' in signature.parameters:
        return _parse_one(sql=sql, dialect=dialect)
    return _parse_one(sql=sql)
