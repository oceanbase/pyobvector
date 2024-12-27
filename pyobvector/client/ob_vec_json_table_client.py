import json
import logging
import re
from typing import Dict, List, Optional, Any

from pydantic import BaseModel, create_model
from sqlalchemy import Column, Integer, String, JSON, Engine, select, text
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlglot import parse_one, exp, Expression

from .ob_vec_client import ObVecClient
from ..json_table import (
    OceanBase,
    ChangeColumn,
    JsonTableBool,
    JsonTableTimestamp,
    JsonTableVarcharFactory,
    JsonTableDecimalFactory,
    JsonTableInt,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ObVecJsonTableClient(ObVecClient):
    """OceanBase Vector Store Client with JSON Table."""

    Base = declarative_base()

    class JsonTableMetaTBL(Base):
        __tablename__ = '_meta_json_t'
        
        user_id = Column(Integer, primary_key=True)
        jtable_name = Column(String(512), primary_key=True)
        jcol_id = Column(Integer, primary_key=True)
        jcol_name = Column(String(512), primary_key=True)
        jcol_type = Column(String(128), nullable=False)
        jcol_nullable = Column(TINYINT, nullable=False)
        jcol_has_default = Column(TINYINT, nullable=False)
        jcol_default = Column(JSON)

    class JsonTableDataTBL(Base):
        __tablename__ = '_data_json_t'

        user_id = Column(Integer, primary_key=True)
        jtable_name = Column(String(512), primary_key=True)
        jdata_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        jdata = Column(JSON)

    class JsonTableMetadata:
        def __init__(self, user_id: int):
            self.user_id = user_id
            self.meta_cache: Dict[str, List] = {}

        @classmethod
        def _parse_col_type(cls, col_type: str):
            if col_type.startswith('TINYINT'):
                return JsonTableBool
            elif col_type.startswith('TIMESTAMP'):
                return JsonTableTimestamp
            elif col_type.startswith('INT'):
                return JsonTableInt
            elif col_type.startswith('VARCHAR'):
                if col_type == 'VARCHAR':
                    factory = JsonTableVarcharFactory(255)
                else:
                    varchar_pattern = r'VARCHAR\((\d+)\)'
                    varchar_matches = re.findall(varchar_pattern, col_type)
                    factory = JsonTableVarcharFactory(int(varchar_matches[0]))
                model = factory.get_json_table_varchar_type()
                return model
            elif col_type.startswith('DECIMAL'):
                if col_type == 'DECIMAL':
                    factory = JsonTableDecimalFactory(10, 0)
                else:
                    decimal_pattern = r'DECIMAL\((\d+),\s*(\d+)\)'
                    decimal_matches = re.findall(decimal_pattern, col_type)
                    x, y = decimal_matches[0]
                    factory = JsonTableDecimalFactory(int(x), int(y))
                model = factory.get_json_table_decimal_type()
                return model
            raise ValueError(f"Invalid column type string: {col_type}")

        def reflect(self, engine: Engine):
            self.meta_cache = {}
            with engine.connect() as conn:
                with conn.begin():
                    stmt = select(ObVecJsonTableClient.JsonTableMetaTBL).filter(
                        ObVecJsonTableClient.JsonTableMetaTBL.user_id == self.user_id
                    )
                    res = conn.execute(stmt)
                    for r in res:
                        if r[1] not in self.meta_cache:
                            self.meta_cache[r[1]] = []
                        self.meta_cache[r[1]].append({
                            'jcol_id': r[2],
                            'jcol_name': r[3],
                            'jcol_type': r[4],
                            'jcol_nullable': bool(r[5]),
                            'jcol_has_default': bool(r[6]),
                            'jcol_default': (
                                r[7]['default']
                                if isinstance(r[7], dict) else
                                json.loads(r[7])['default']
                            ),
                            'jcol_model': ObVecJsonTableClient.JsonTableMetadata._parse_col_type(r[4])
                        })
                    for k, _ in self.meta_cache.items():
                        self.meta_cache[k].sort(key=lambda x: x['jcol_id'])

                    for k, v in self.meta_cache.items():
                        logger.info(f"LOAD TABLE --- {k}: {v}")


    def __init__(
        self,
        user_id: int,
        uri: str = "127.0.0.1:2881",
        user: str = "root@test",
        password: str = "",
        db_name: str = "test",
        **kwargs,
    ):
        super().__init__(uri, user, password, db_name, **kwargs)
        self.Base.metadata.create_all(self.engine)
        self.user_id = user_id
        self.jmetadata = ObVecJsonTableClient.JsonTableMetadata(self.user_id)
        self.jmetadata.reflect(self.engine)

    def _reset(self):
        # Only for test
        self.perform_raw_text_sql("TRUNCATE TABLE _data_json_t")
        self.perform_raw_text_sql("TRUNCATE TABLE _meta_json_t")
        self.jmetadata = ObVecJsonTableClient.JsonTableMetadata(self.user_id)
    
    def refresh_metadata(self):
        self.jmetadata.reflect(self.engine)

    def perform_json_table_sql(self, sql: str):
        """Perform common SQL that operates on JSON Table."""
        ast = parse_one(sql, dialect="oceanbase")
        if isinstance(ast, exp.Create):
            if ast.kind and ast.kind == 'TABLE':
                self._handle_create_json_table(ast)
            else:
                raise ValueError(f"Create {ast.kind} is not supported")
        elif isinstance(ast, exp.Alter):
            self._handle_alter_json_table(ast)
        elif isinstance(ast, exp.Insert):
            self._handle_jtable_dml_insert(ast)
        
    def _parse_datatype_to_str(self, datatype):
        if datatype == exp.DataType.Type.INT:
            return "INT"
        if datatype == exp.DataType.Type.TINYINT:
            return "TINYINT"
        if datatype == exp.DataType.Type.TIMESTAMP:
            return "TIMESTAMP"
        if datatype == exp.DataType.Type.VARCHAR:
            return "VARCHAR"
        if datatype == exp.DataType.Type.DECIMAL:
            return "DECIMAL"
        raise ValueError(f"{datatype} not supported")
    
    def _calc_default_value(self, default_val):
        with self.engine.connect() as conn:
            res = conn.execute(text(f"SELECT {default_val}"))
            for r in res:
                logger.info(f"============== Calculate default value: {r[0]}")
                return r[0]
    
    def _handle_create_json_table(self, ast: Expression):
        logger.info("HANDLE CREATE JSON TABLE")

        if not isinstance(ast.this, exp.Schema):
            raise ValueError("Invalid create table statement")
        schema = ast.this
        if not isinstance(schema.this, exp.Table):
            raise ValueError("Invalid create table statement")
        jtable = schema.this
        if not isinstance(jtable.this, exp.Identifier):
            raise ValueError("Invalid create table statement")
        jtable_name = jtable.this.this
        logger.info(jtable_name)

        if jtable_name in self.jmetadata.meta_cache:
            raise ValueError("Table name duplicated")
        
        Session = sessionmaker(bind=self.engine)
        session = Session()
        new_meta_cache_items = []
        col_id = 16
        for col_def in ast.find_all(exp.ColumnDef):
            col_name = col_def.this.this
            col_type_str = self._parse_datatype_to_str(col_def.kind.this)
            col_type_params = col_def.kind.expressions
            col_type_params_list = []
            col_nullable = True
            col_has_default = False
            col_default_val = None
            for param in col_type_params:
                if param.is_string:
                    col_type_params_list.append(f"'{param.this}'")
                else:
                    col_type_params_list.append(f"{param.this}")
            if len(col_type_params_list) > 0:
                col_type_str += '(' + ','.join(col_type_params_list) + ')'
            col_type_model = ObVecJsonTableClient.JsonTableMetadata._parse_col_type(col_type_str)
            
            for cons in col_def.constraints:
                if isinstance(cons.kind, exp.DefaultColumnConstraint):
                    col_has_default = True
                    logger.info(f"############ create jtable ########### {str(cons.kind.this)}")
                    col_default_val = str(cons.kind.this)
                    if col_default_val.upper() == "NULL":
                        col_default_val = None
                elif isinstance(cons.kind, exp.NotNullColumnConstraint):
                    col_nullable = False
                else:
                    raise ValueError(f"{cons.kind} constriaint is not supported.")
            
            if col_has_default and (col_default_val is not None):
                # check default value is valid
                col_type_model(val=self._calc_default_value(col_default_val))

            if (not col_nullable) and col_has_default and (col_default_val is None):
                raise ValueError(f"Invalid default value for '{col_name}'")

            logger.info(
                f"col_name={col_name}, col_id={col_id}, "
                f"col_type_str={col_type_str}, col_nullable={col_nullable}, "
                f"col_has_default={col_has_default}, col_default_val={col_default_val}"
            )
            new_meta_cache_items.append({
                'jcol_id': col_id,
                'jcol_name': col_name,
                'jcol_type': col_type_str,
                'jcol_nullable': col_nullable,
                'jcol_has_default': col_has_default,
                'jcol_default': col_default_val,
                'jcol_model': col_type_model,
            })
            session.add(ObVecJsonTableClient.JsonTableMetaTBL(
                user_id = self.user_id,
                jtable_name = jtable_name,
                jcol_id = col_id,
                jcol_name = col_name,
                jcol_type = col_type_str,
                jcol_nullable = col_nullable,
                jcol_has_default = col_has_default,
                jcol_default = {
                    'default': col_default_val,
                }
            ))
            
            col_id += 1
        
        try:
            session.commit()
            self.jmetadata.meta_cache[jtable_name] = new_meta_cache_items
            logger.info(f"ADD METADATA CACHE ---- {jtable_name}: {new_meta_cache_items}")
        except Exception as e:
            session.rollback()
            logger.error(f"Error occurred: {e}")
        finally:
            session.close()

    def _check_table_exists(self, jtable_name: str) -> bool:
        return jtable_name in self.jmetadata.meta_cache
    
    def _check_col_exists(self, jtable_name: str, col_name: str) -> Optional[Dict]:
        if not self._check_table_exists(jtable_name):
            return None
        for col_meta in self.jmetadata.meta_cache[jtable_name]:
            if col_meta['jcol_name'] == col_name:
                return col_meta
        return None
    
    def _parse_col_datatype(self, expr: Expression) -> str:
        col_type_str = self._parse_datatype_to_str(expr.this)
        col_type_params_list = []
        for param in expr.expressions:
            if param.is_string:
                col_type_params_list.append(f"'{param.this}'")
            else:
                col_type_params_list.append(f"{param.this}")
        if len(col_type_params_list) > 0:
            col_type_str += '(' + ','.join(col_type_params_list) + ')'
        return col_type_str
    
    def _parse_col_constraints(self, expr: Expression) -> Dict:
        col_has_default = False
        col_nullable = True
        for cons in expr:
            if isinstance(cons.kind, exp.DefaultColumnConstraint):
                col_has_default = True
                logger.info(f"############ column constraints ########### {str(cons.kind.this)}")
                col_default_val = str(cons.kind.this)
                if col_default_val.upper() == "NULL":
                    col_default_val = None
            elif isinstance(cons.kind, exp.NotNullColumnConstraint):
                col_nullable = False
            else:
                raise ValueError(f"{cons.kind} constriaint is not supported.")
        return {
            'jcol_nullable': col_nullable,
            'jcol_has_default': col_has_default,
            'jcol_default': col_default_val,
        }

    def _handle_alter_jtable_change_column(
        self,
        session: Session,
        jtable_name: str,
        change_col: Expression,
    ):
        logger.info("HANDLE ALTER CHANGE COLUMN")
        origin_col_name = change_col.origin_col_name.this
        if not self._check_col_exists(jtable_name, origin_col_name):
            raise ValueError(f"{origin_col_name} not exists in {jtable_name}")
        
        new_col_name = change_col.this
        if self._check_col_exists(jtable_name, new_col_name):
            raise ValueError(f"Column {new_col_name} exists!")
        
        col_type_str = self._parse_col_datatype(change_col.dtype)

        session.query(ObVecJsonTableClient.JsonTableMetaTBL).filter_by(
            user_id=self.user_id,
            jtable_name=jtable_name,
            jcol_name=origin_col_name
        ).update({
            ObVecJsonTableClient.JsonTableMetaTBL.jcol_name: new_col_name,
            ObVecJsonTableClient.JsonTableMetaTBL.jcol_type: col_type_str,
            ObVecJsonTableClient.JsonTableMetaTBL.jcol_nullable: True,
            ObVecJsonTableClient.JsonTableMetaTBL.jcol_has_default: True,
            ObVecJsonTableClient.JsonTableMetaTBL.jcol_default: {
                'default': None
            },
        })

        # TODO: update json data


    def _handle_alter_jtable_drop_column(
        self,
        session: Session,
        jtable_name: str,
        drop_col: Expression,
    ):
        logger.info("HANDLE ALTER DROP COLUMN")
        if not isinstance(drop_col.this, exp.Column):
            raise ValueError(f"Drop {drop_col.kind} is not supported")
        col_name = drop_col.this.this.this
        if not self._check_col_exists(jtable_name, col_name):
            raise ValueError(f"{col_name} not exists in {jtable_name}")

        session.query(ObVecJsonTableClient.JsonTableMetaTBL).filter_by(
            user_id=self.user_id,
            jtable_name=jtable_name,
            jcol_name=col_name
        ).delete()

        # TODO: update json data

    def _handle_alter_jtable_add_column(
        self,
        session: Session,
        jtable_name: str,
        add_col: Expression,
    ):
        logger.info("HANDLE ALTER ADD COLUMN")
        new_col_name = add_col.this.this
        if self._check_col_exists(jtable_name, new_col_name):
            raise ValueError(f"{new_col_name} exists!")
        
        col_type_str = self._parse_col_datatype(add_col.kind)
        model = ObVecJsonTableClient.JsonTableMetadata._parse_col_type(col_type_str)
        constraints = self._parse_col_constraints(add_col.constraints)
        if constraints['jcol_has_default'] and (constraints['jcol_default'] is not None):
            model(val=self._calc_default_value(constraints['jcol_default']))
        cur_col_id = max([meta['jcol_id'] for meta in self.jmetadata.meta_cache[jtable_name]]) + 1

        session.add(ObVecJsonTableClient.JsonTableMetaTBL(
            user_id = self.user_id,
            jtable_name = jtable_name,
            jcol_id = cur_col_id,
            jcol_name = new_col_name,
            jcol_type = col_type_str,
            jcol_nullable = constraints['jcol_nullable'],
            jcol_has_default = constraints['jcol_has_default'],
            jcol_default = {
                'default': constraints['jcol_default'],
            }
        ))

        # TODO: update json data

    def _handle_alter_jtable_modify_column(
        self,
        session: Session,
        jtable_name: str,
        modify_col: Expression,
    ):
        logger.info("HANDLE ALTER MODIFY COLUMN")
        col_def = modify_col.this
        col_name = col_def.this.this
        if not self._check_col_exists(jtable_name, col_name):
            raise ValueError(f"{col_name} not exists in {jtable_name}")
        
        col_type_str = self._parse_col_datatype(col_def.kind)
        model = ObVecJsonTableClient.JsonTableMetadata._parse_col_type(col_type_str)
        constraints = self._parse_col_constraints(col_def.constraints)
        if constraints['jcol_has_default'] and (constraints['jcol_default'] is not None):
            model(val=self._calc_default_value(constraints['jcol_default']))

        session.query(ObVecJsonTableClient.JsonTableMetaTBL).filter_by(
            user_id=self.user_id,
            jtable_name=jtable_name,
            jcol_name=col_name
        ).update({
            ObVecJsonTableClient.JsonTableMetaTBL.jcol_name: col_name,
            ObVecJsonTableClient.JsonTableMetaTBL.jcol_type: col_type_str,
            ObVecJsonTableClient.JsonTableMetaTBL.jcol_nullable: constraints['jcol_nullable'],
            ObVecJsonTableClient.JsonTableMetaTBL.jcol_has_default: constraints['jcol_has_default'],
            ObVecJsonTableClient.JsonTableMetaTBL.jcol_default: {
                'default': constraints['jcol_default']
            },
        })

        # TODO: update json data

    def _handle_alter_jtable_rename_table(
        self,
        session: Session,
        jtable_name: str,
        rename: Expression,
    ):
        if not self._check_table_exists(jtable_name):
            raise ValueError(f"Table {jtable_name} does not exists")
        
        new_table_name = rename.this.this.this
        if self.check_table_exists(new_table_name):
            raise ValueError(f"Table {new_table_name} exists!")
        
        session.query(ObVecJsonTableClient.JsonTableMetaTBL).filter_by(
            user_id=self.user_id,
            jtable_name=jtable_name,
        ).update({
            ObVecJsonTableClient.JsonTableMetaTBL.jtable_name: new_table_name,
        })

    def _handle_alter_json_table(self, ast: Expression):
        if not isinstance(ast.this, exp.Table):
            raise ValueError("Invalid alter table statement")
        if not isinstance(ast.this.this, exp.Identifier):
            raise ValueError("Invalid create table statement")
        jtable_name = ast.this.this.this
        if not self._check_table_exists(jtable_name):
            raise ValueError(f"Table {jtable_name} does not exists")
        
        Session = sessionmaker(bind=self.engine)
        session = Session()
        for action in ast.actions:
            if isinstance(action, ChangeColumn):
                self._handle_alter_jtable_change_column(
                    session,
                    jtable_name,
                    action,
                )
            if isinstance(action, exp.Drop):
                self._handle_alter_jtable_drop_column(
                    session,
                    jtable_name,
                    action,
                )
            if isinstance(action, exp.AlterColumn):
                self._handle_alter_jtable_modify_column(
                    session,
                    jtable_name,
                    action,
                )
            if isinstance(action, exp.ColumnDef):
                self._handle_alter_jtable_add_column(
                    session,
                    jtable_name,
                    action,
                )
            if isinstance(action, exp.AlterRename):
                self._handle_alter_jtable_rename_table(
                    session,
                    jtable_name,
                    action,
                )
        
        try:
            session.commit()
            self.jmetadata.reflect(self.engine)
        except Exception as e:
            session.rollback()
            logger.error(f"Error occurred: {e}")
        finally:
            session.close()

    def _handle_jtable_dml_insert(self, ast: Expression):
        if isinstance(ast.this, exp.Schema):
            table_name = ast.this.this.this.this
            insert_col_names = [expr.this for expr in ast.this.expressions]
            table_col_names = [meta['jcol_name'] for meta in self.jmetadata.meta_cache[table_name]]
            for col_name in insert_col_names:
                if col_name not in table_col_names:
                    raise ValueError(f"Unknown column {col_name} in field list")
            cols = []
            for meta in self.jmetadata.meta_cache[table_name]:
                if ((meta['jcol_name'] not in insert_col_names) and
                    (not meta['jcol_nullable']) and (not meta['jcol_has_default'])):
                    raise ValueError(f"Field {meta['jcol_name']} does not have a default value")
                cols.append(meta)
        elif isinstance(ast.this, exp.Table):
            table_name = ast.this.this.this
            cols = self.jmetadata.meta_cache[table_name]

        for tuple in ast.expression.expressions:
            
            pass
