"""A module to specify fts index parameters"""
from enum import Enum
from typing import List, Optional

class FtsParser(Enum):
    IK = 0
    NGRAM = 1


class FtsIndexParam:
    def __init__(
        self,
        index_name: str,
        field_names: List[str],
        parser_type: Optional[FtsParser],
    ):
        self.index_name = index_name
        self.field_names = field_names
        self.parser_type = parser_type

    def param_str(self) -> str:
        if self.parser_type is None:
            return None
        if self.parser_type == FtsParser.IK:
            return "ik"
        if self.parser_type == FtsParser.NGRAM:
            return "ngram"

    def __iter__(self):
        yield "index_name", self.index_name
        yield "field_names", self.field_names
        if self.parser_type:
            yield "parser_type", self.parser_type

    def __str__(self):
        return str(dict(self))

    def __eq__(self, other: None):
        if isinstance(other, self.__class__):
            return dict(self) == dict(other)

        if isinstance(other, dict):
            return dict(self) == other
        return False
