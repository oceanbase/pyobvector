"""OceanBase table definition reflection."""
import re
import logging
from sqlalchemy.dialects.mysql.reflection import MySQLTableDefinitionParser, _re_compile, cleanup_text

from pyobvector.schema.array import nested_array

logger = logging.getLogger(__name__)

class OceanBaseTableDefinitionParser(MySQLTableDefinitionParser):
    """OceanBase table definition parser."""
    def __init__(self, dialect, preparer, *, default_schema=None):
        MySQLTableDefinitionParser.__init__(self, dialect, preparer)
        self.default_schema = default_schema

    def _prep_regexes(self):
        super()._prep_regexes()

        ### this block is copied from MySQLTableDefinitionParser._prep_regexes
        _final = self.preparer.final_quote
        quotes = dict(
            zip(
                ("iq", "fq", "esc_fq"),
                [
                    re.escape(s)
                    for s in (
                        self.preparer.initial_quote,
                        _final,
                        self.preparer._escape_identifier(_final),
                    )
                ],
            )
        )
        ### end of block

        self._re_array_column = _re_compile(
            r"\s*"
            r"%(iq)s(?P<name>(?:%(esc_fq)s|[^%(fq)s])+)%(fq)s\s+"
            r"(?P<coltype_with_args>(?i:(?<!\w)array(?!\w))\s*\([^()]*(?:\([^()]*\)[^()]*)*\))"
            r"(?:\s+(?P<notnull>(?:NOT\s+)?NULL))?"
            r"(?:\s+DEFAULT\s+(?P<default>(?:NULL|'(?:''|[^'])*'|\(.+?\)|[\-\w\.\(\)]+)))?"
            r"(?:\s+COMMENT\s+'(?P<comment>(?:''|[^'])*)')?"
            r"\s*,?\s*$" % quotes
        )

        self._re_key = _re_compile(
            r"  "
            r"(?:(FULLTEXT|SPATIAL|VECTOR|(?P<type>\S+)) )?KEY"
            # r"(?:(?P<type>\S+) )?KEY"
            r"(?: +{iq}(?P<name>(?:{esc_fq}|[^{fq}])+){fq})?"
            r"(?: +USING +(?P<using_pre>\S+))?"
            r" +\((?P<columns>.+?)\)"
            r"(?: +USING +(?P<using_post>\S+))?"
            r"(?: +(KEY_)?BLOCK_SIZE *[ =]? *(?P<keyblock>\S+) *(LOCAL)?)?"
            r"(?: +WITH PARSER +(?P<parser>\S+))?"
            r"(?: +COMMENT +(?P<comment>(\x27\x27|\x27([^\x27])*?\x27)+))?"
            r"(?: +/\*(?P<version_sql>.+)\*/ *)?"
            r",?$".format(iq=quotes["iq"], esc_fq=quotes["esc_fq"], fq=quotes["fq"])
        )

        kw = quotes.copy()
        kw["on"] = "RESTRICT|CASCADE|SET NULL|NO ACTION"
        self._re_fk_constraint = _re_compile(
            r"  "
            r"CONSTRAINT +"
            r"{iq}(?P<name>(?:{esc_fq}|[^{fq}])+){fq} +"
            r"FOREIGN KEY +"
            r"\((?P<local>[^\)]+?)\) REFERENCES +"
            r"(?P<table>{iq}[^{fq}]+{fq}"
            r"(?:\.{iq}[^{fq}]+{fq})?) *"
            r"\((?P<foreign>(?:{iq}[^{fq}]+{fq}(?: *, *)?)+)\)"
            r"(?: +(?P<match>MATCH \w+))?"
            r"(?: +ON UPDATE (?P<onupdate>{on}))?"
            r"(?: +ON DELETE (?P<ondelete>{on}))?".format(
                iq=quotes["iq"], esc_fq=quotes["esc_fq"], fq=quotes["fq"], on=kw["on"]
            )
        )

    def _parse_column(self, line, state):
        m = self._re_array_column.match(line)
        if m:
            spec = m.groupdict()
            name, coltype_with_args = spec["name"].strip(), spec["coltype_with_args"].strip()

            item_pattern = re.compile(
                r"^(?:array\s*\()*([\w]+)(?:\(([\d,]+)\))?\)*$",
                re.IGNORECASE
            )
            item_m = item_pattern.match(coltype_with_args)
            if not item_m:
                raise ValueError(f"Failed to find inner type from array column definition: {line}")

            item_type = self.dialect.ischema_names[item_m.group(1).lower()]
            item_type_arg = item_m.group(2)
            if item_type_arg is None or item_type_arg == "":
                item_type_args = []
            elif item_type_arg[0] == "'" and item_type_arg[-1] == "'":
                item_type_args = self._re_csv_str.findall(item_type_arg)
            else:
                item_type_args = [int(v) for v in self._re_csv_int.findall(item_type_arg)]

            nested_level = coltype_with_args.lower().count('array')
            type_instance = nested_array(nested_level)(item_type(*item_type_args))

            col_kw = {}

            # NOT NULL
            col_kw["nullable"] = True
            if spec.get("notnull", False) == "NOT NULL":
                col_kw["nullable"] = False

            # DEFAULT
            default = spec.get("default", None)

            if default == "NULL":
                # eliminates the need to deal with this later.
                default = None

            comment = spec.get("comment", None)

            if comment is not None:
                comment = cleanup_text(comment)

            col_d = dict(
                name=name, type=type_instance, default=default, comment=comment
            )
            col_d.update(col_kw)
            state.columns.append(col_d)
        else:
            super()._parse_column(line, state)

    def _parse_constraints(self, line):
        """Parse a CONSTRAINT line."""
        ret = super()._parse_constraints(line)
        if ret:
            tp, spec = ret

            if tp is None:
                return ret
            if tp == "partition":
                # do not handle partition
                return ret
            if tp == "fk_constraint":
                if len(spec["table"]) == 2 and spec["table"][0] == self.default_schema:
                    spec["table"] = spec["table"][1:]
            if spec.get("onupdate", "").lower() == "restrict":
                spec["onupdate"] = None
            if spec.get("ondelete", "").lower() == "restrict":
                spec["ondelete"] = None
        return ret
