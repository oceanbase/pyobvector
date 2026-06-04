"""A module to specify fts index parameters"""

import json
from enum import Enum


class FtsParser(Enum):
    """Built-in full-text search parser types supported by OceanBase"""

    IK = 0
    NGRAM = 1
    NGRAM2 = 2  # NGRAM2 parser (supported from V4.3.5 BP2+)
    BASIC_ENGLISH = 3  # Basic English parser
    JIEBA = 4  # jieba parser
    ANALYZER = 5  # analyzer parser with PARSER_PROPERTIES support


class FtsIndexParam:
    """Full-text search index parameter.

    Args:
        index_name: Index name
        field_names: List of field names to create full-text index on
        parser_properties: Content placed inside PARSER_PROPERTIES = (...) in the DDL.
                           When set and parser_type is not None, the clause is appended.
                           Required for FtsParser.ANALYZER; optional for others.
    """

    def __init__(
        self,
        index_name: str,
        field_names: list[str],
        parser_type: FtsParser | str | None = None,
        parser_properties: str | None = None,
    ):
        self.index_name = index_name
        self.field_names = field_names
        self.parser_type = parser_type
        self.parser_properties = parser_properties

    def param_str(self) -> str | None:
        """Convert parser type to string format for SQL."""
        if self.parser_type is None:
            return None  # Default Space parser, no need to specify

        if isinstance(self.parser_type, str):
            # Custom parser name (e.g., "thai_ftparser")
            if (
                self.parser_type.lower() == "analyzer"
                and self.parser_properties is None
            ):
                raise ValueError(
                    'parser_type "analyzer" requires parser_properties '
                    "(OceanBase rejects WITH PARSER analyzer without PARSER_PROPERTIES). "
                    'Example value: analysis = \'{"analyzer": "standard"}\''
                )
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
            if self.parser_type == FtsParser.ANALYZER:
                if self.parser_properties is None:
                    raise ValueError(
                        "FtsParser.ANALYZER requires parser_properties "
                        "(OceanBase rejects WITH PARSER analyzer without PARSER_PROPERTIES). "
                        'Example value: analysis = \'{"analyzer": "standard"}\''
                    )
                return "analyzer"
            # Raise exception for unrecognized FtsParser enum values
            raise ValueError(f"Unrecognized FtsParser enum value: {self.parser_type}")

        return None

    def __iter__(self):
        yield "index_name", self.index_name
        yield "field_names", self.field_names
        if self.parser_type:
            yield "parser_type", self.parser_type
        if self.parser_properties is not None:
            yield "parser_properties", self.parser_properties

    def __str__(self):
        return str(dict(self))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return dict(self) == dict(other)

        if isinstance(other, dict):
            return dict(self) == other
        return False


def make_analyzer_properties(analyzer_type: str = "standard") -> str:
    """Build the parser_properties string for a built-in analyzer parser.

    Args:
        analyzer_type: Analyzer name. Defaults to "standard".

    Returns:
        A string suitable for FtsIndexParam(parser_properties=...), e.g.
        ``analysis = '{"analyzer": "standard"}'``
    """
    return f"analysis = '{json.dumps({'analyzer': analyzer_type})}'"
