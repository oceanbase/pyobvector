"""A module to specify fts index parameters"""
from enum import Enum
from typing import List, Optional, Union

class FtsParser(Enum):
    """Built-in full-text search parser types supported by OceanBase"""
    IK = 0
    NGRAM = 1
    NGRAM2 = 2  # NGRAM2 parser (supported from V4.3.5 BP2+)
    BASIC_ENGLISH = 3  # Basic English parser
    JIEBA = 4  # jieba parser


class FtsIndexParam:
    """Full-text search index parameter.
    
    Args:
        index_name: Index name
        field_names: List of field names to create full-text index on
        parser_type: Parser type, can be FtsParser enum or string (for custom parsers)
                    If None, uses default Space parser
    """
    def __init__(
        self,
        index_name: str,
        field_names: List[str],
        parser_type: Optional[Union[FtsParser, str]] = None,
    ):
        self.index_name = index_name
        self.field_names = field_names
        self.parser_type = parser_type

    def param_str(self) -> Optional[str]:
        """Convert parser type to string format for SQL."""
        if self.parser_type is None:
            return None  # Default Space parser, no need to specify
        
        if isinstance(self.parser_type, str):
            # Custom parser name (e.g., "thai_ftparser")
            return self.parser_type.lower()
        
        if isinstance(self.parser_type, FtsParser):
            if self.parser_type == FtsParser.IK:
                return "ik"
            if self.parser_type == FtsParser.NGRAM:
                return "ngram"
            if self.parser_type == FtsParser.NGRAM2:
                return "ngram2"
            if self.parser_type == FtsParser.BASIC_ENGLISH:
                return "beng"
            if self.parser_type == FtsParser.JIEBA:
                return "jieba"
            # Raise exception for unrecognized FtsParser enum values
            raise ValueError(f"Unrecognized FtsParser enum value: {self.parser_type}")
        
        return None

    def __iter__(self):
        yield "index_name", self.index_name
        yield "field_names", self.field_names
        if self.parser_type:
            yield "parser_type", self.parser_type

    def __str__(self):
        return str(dict(self))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return dict(self) == dict(other)

        if isinstance(other, dict):
            return dict(self) == other
        return False
