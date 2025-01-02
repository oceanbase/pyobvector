import logging
import re
from typing import Tuple

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy import BINARY, Float, Boolean, Text

logger = logging.getLogger(__name__)

class json_value(FunctionElement):
    type = Text()
    inherit_cache = True

    def __init__(self, *args):
        super().__init__()
        self.args = args

@compiles(json_value)
def compile_json_value(element, compiler, **kwargs):
    args = []
    if len(element.args) != 3:
        raise ValueError("Number of args for json_value should be 3")
    args.append(compiler.process(element.args[0]))
    if not (isinstance(element.args[1], str) and isinstance(element.args[2], str)):
        raise ValueError("Invalid args for json_value")
    
    if element.args[2].startswith('TINYINT'):
        returning_type = "SIGNED"
    elif element.args[2].startswith('TIMESTAMP'):
        returning_type = "DATETIME"
    elif element.args[2].startswith('INT'):
        returning_type = "SIGNED"
    elif element.args[2].startswith('VARCHAR'):
        if element.args[2] == 'VARCHAR':
            returning_type = "CHAR(255)"
        else:
            varchar_pattern = r'VARCHAR\((\d+)\)'
            varchar_matches = re.findall(varchar_pattern, element.args[2])
            returning_type = f"CHAR({int(varchar_matches[0])})"
    elif element.args[2].startswith('DECIMAL'):
        if element.args[2] == 'DECIMAL':
            returning_type = "DECIMAL(10, 0)"
        else:
            decimal_pattern = r'DECIMAL\((\d+),\s*(\d+)\)'
            decimal_matches = re.findall(decimal_pattern, element.args[2])
            x, y = decimal_matches[0]
            returning_type = f"DECIMAL({x}, {y})"
    args.append(f"'{element.args[1]}' RETURNING {returning_type}")
    args = ", ".join(args)
    return f"json_value({args})"
