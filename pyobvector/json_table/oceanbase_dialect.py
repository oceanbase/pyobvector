import typing as t
from sqlglot import parser, exp, Expression
from sqlglot.dialects.mysql import MySQL
from sqlglot.tokens import TokenType

class ChangeColumn(Expression):
    arg_types = {
        "this": True,
        "origin_col_name": True,
        "dtype": True,
    }

    @property
    def origin_col_name(self) -> str:
        origin_col_name = self.args.get("origin_col_name")
        return origin_col_name
    
    @property
    def dtype(self) -> Expression:
        dtype = self.args.get("dtype")
        return dtype

class OceanBase(MySQL):
    class Parser(MySQL.Parser):
        ALTER_PARSERS = {
            **parser.Parser.ALTER_PARSERS,
            "MODIFY": lambda self: self._parse_alter_table_alter(),
            "CHANGE": lambda self: self._parse_change_table_column(),
        }
        
        def _parse_alter_table_alter(self) -> t.Optional[exp.Expression]:
            if self._match_texts(self.ALTER_ALTER_PARSERS):
                return self.ALTER_ALTER_PARSERS[self._prev.text.upper()](self)

            self._match(TokenType.COLUMN)
            column = self._parse_field_def()

            if self._match_pair(TokenType.DROP, TokenType.DEFAULT):
                return self.expression(exp.AlterColumn, this=column, drop=True)
            if self._match_pair(TokenType.SET, TokenType.DEFAULT):
                return self.expression(exp.AlterColumn, this=column, default=self._parse_assignment())
            if self._match(TokenType.COMMENT):
                return self.expression(exp.AlterColumn, this=column, comment=self._parse_string())
            if self._match_text_seq("DROP", "NOT", "NULL"):
                return self.expression(
                    exp.AlterColumn,
                    this=column,
                    drop=True,
                    allow_null=True,
                )
            if self._match_text_seq("SET", "NOT", "NULL"):
                return self.expression(
                    exp.AlterColumn,
                    this=column,
                    allow_null=False,
                )
            self._match_text_seq("SET", "DATA")
            self._match_text_seq("TYPE")
            return self.expression(
                exp.AlterColumn,
                this=column,
                dtype=self._parse_types(),
                collate=self._match(TokenType.COLLATE) and self._parse_term(),
                using=self._match(TokenType.USING) and self._parse_assignment(),
            )
        
        def _parse_drop(self, exists: bool = False) -> exp.Drop | exp.Command:
            temporary = self._match(TokenType.TEMPORARY)
            materialized = self._match_text_seq("MATERIALIZED")

            kind = self._match_set(self.CREATABLES) and self._prev.text.upper()
            if not kind:
                kind = "COLUMN"

            concurrently = self._match_text_seq("CONCURRENTLY")
            if_exists = exists or self._parse_exists()

            if kind == "COLUMN":
                this = self._parse_column()
            else:
                this = self._parse_table_parts(
                    schema=True, is_db_reference=self._prev.token_type == TokenType.SCHEMA
                )

            cluster = self._parse_on_property() if self._match(TokenType.ON) else None

            if self._match(TokenType.L_PAREN, advance=False):
                expressions = self._parse_wrapped_csv(self._parse_types)
            else:
                expressions = None

            return self.expression(
                exp.Drop,
                exists=if_exists,
                this=this,
                expressions=expressions,
                kind=self.dialect.CREATABLE_KIND_MAPPING.get(kind) or kind,
                temporary=temporary,
                materialized=materialized,
                cascade=self._match_text_seq("CASCADE"),
                constraints=self._match_text_seq("CONSTRAINTS"),
                purge=self._match_text_seq("PURGE"),
                cluster=cluster,
                concurrently=concurrently,
            )
        
        def _parse_change_table_column(self) -> t.Optional[exp.Expression]:
            self._match(TokenType.COLUMN)
            origin_col = self._parse_field(any_token=True)
            column = self._parse_field()
            return self.expression(
                ChangeColumn,
                this=column,
                origin_col_name=origin_col,
                dtype=self._parse_types(),
            )