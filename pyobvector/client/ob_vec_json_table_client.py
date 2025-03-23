import json
import logging
import re
from typing import Dict, List, Optional, Any

from sqlalchemy import Column, Integer, String, JSON, Engine, select, text, func, CursorResult
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlglot import parse_one, exp, Expression, to_identifier
from sqlglot.expressions import Concat


from .ob_vec_client import ObVecClient
from ..json_table import (
    OceanBase,
    ChangeColumn,
    JsonTableBool,
    JsonTableTimestamp,
    JsonTableVarcharFactory,
    JsonTableDecimalFactory,
    JsonTableInt,
    val2json,
    json_value
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

JSON_TABLE_META_TABLE_NAME = "_meta_json_t"
JSON_TABLE_DATA_TABLE_NAME = "_data_json_t"

class ObVecJsonTableClient(ObVecClient):
    """OceanBase Vector Store Client with JSON Table."""

    Base = declarative_base()

    class JsonTableMetaTBL(Base):
        __tablename__ = JSON_TABLE_META_TABLE_NAME
        
        user_id = Column(String(128), primary_key=True, autoincrement=False)
        jtable_name = Column(String(512), primary_key=True)
        jcol_id = Column(Integer, primary_key=True)
        jcol_name = Column(String(512), primary_key=True)
        jcol_type = Column(String(128), nullable=False)
        jcol_nullable = Column(TINYINT, nullable=False)
        jcol_has_default = Column(TINYINT, nullable=False)
        jcol_default = Column(JSON)

    class JsonTableDataTBL(Base):
        __tablename__ = JSON_TABLE_DATA_TABLE_NAME

        user_id = Column(String(128), primary_key=True, autoincrement=False)
        jtable_name = Column(String(512), primary_key=True)
        jdata_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        jdata = Column(JSON)

    class JsonTableMetadata:
        def __init__(self, user_id: str):
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
                    decimal_pattern = r'DECIMAL\s*\((\d+),\s*(\d+)\)'
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
                        logger.debug(f"LOAD TABLE --- {k}: {v}")


    def __init__(
        self,
        user_id: str,
        uri: str = "127.0.0.1:2881",
        user: str = "root@test",
        password: str = "",
        db_name: str = "test",
        **kwargs,
    ):
        super().__init__(uri, user, password, db_name, **kwargs)
        self.Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)
        self.user_id = user_id
        self.jmetadata = ObVecJsonTableClient.JsonTableMetadata(self.user_id)
        self.jmetadata.reflect(self.engine)

    def _reset(self):
        # Only for test
        self.perform_raw_text_sql(f"TRUNCATE TABLE {JSON_TABLE_DATA_TABLE_NAME}")
        self.perform_raw_text_sql(f"TRUNCATE TABLE {JSON_TABLE_META_TABLE_NAME}")
        self.jmetadata = ObVecJsonTableClient.JsonTableMetadata(self.user_id)
    
    def refresh_metadata(self) -> None:
        self.jmetadata.reflect(self.engine)

    def perform_json_table_sql(self, sql: str) -> Optional[CursorResult]:
        """Perform common SQL that operates on JSON Table."""
        ast = parse_one(sql, dialect="oceanbase")
        if isinstance(ast, exp.Create):
            if ast.kind and ast.kind == 'TABLE':
                self._handle_create_json_table(ast)
            else:
                raise ValueError(f"Create {ast.kind} is not supported")
            return None
        elif isinstance(ast, exp.Alter):
            self._handle_alter_json_table(ast)
            return None
        elif isinstance(ast, exp.Insert):
            self._handle_jtable_dml_insert(ast)
            return None
        elif isinstance(ast, exp.Update):
            self._handle_jtable_dml_update(ast)
            return None
        elif isinstance(ast, exp.Delete):
            self._handle_jtable_dml_delete(ast)
            return None
        elif isinstance(ast, exp.Select):
            return self._handle_jtable_dml_select(ast)
        else:
            raise ValueError(f"{type(ast)} not supported")
        
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
        if default_val is None:
            return None
        with self.engine.connect() as conn:
            res = conn.execute(text(f"SELECT {default_val}"))
            for r in res:
                logger.debug(f"============== Calculate default value: {r[0]}")
                return r[0]
    
    def _handle_create_json_table(self, ast: Expression):
        logger.debug("HANDLE CREATE JSON TABLE")

        if not isinstance(ast.this, exp.Schema):
            raise ValueError("Invalid create table statement")
        schema = ast.this
        if not isinstance(schema.this, exp.Table):
            raise ValueError("Invalid create table statement")
        jtable = schema.this
        if not isinstance(jtable.this, exp.Identifier):
            raise ValueError("Invalid create table statement")
        jtable_name = jtable.this.this

        if jtable_name == JSON_TABLE_META_TABLE_NAME or jtable_name == JSON_TABLE_DATA_TABLE_NAME:
            raise ValueError(f"Invalid table name: {jtable_name}")
        if jtable_name in self.jmetadata.meta_cache:
            raise ValueError("Table name duplicated")
        
        session = self.session()
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
                    logger.debug(f"############ create jtable ########### {str(cons.kind.this)}")
                    col_default_val = str(cons.kind.this)
                    if col_default_val.upper() == "NULL":
                        col_default_val = None
                elif isinstance(cons.kind, exp.NotNullColumnConstraint):
                    col_nullable = False
                else:
                    pass
                    # raise ValueError(f"{cons.kind} constriaint is not supported.")
                    # TODO support json index
            
            if col_has_default and (col_default_val is not None):
                # check default value is valid
                col_type_model(val=self._calc_default_value(col_default_val))

            if (not col_nullable) and col_has_default and (col_default_val is None):
                raise ValueError(f"Invalid default value for '{col_name}'")

            logger.debug(
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
            logger.debug(f"ADD METADATA CACHE ---- {jtable_name}: {new_meta_cache_items}")
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
                logger.debug(f"############ column constraints ########### {str(cons.kind.this)}")
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
        logger.debug("HANDLE ALTER CHANGE COLUMN")
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

        session.query(ObVecJsonTableClient.JsonTableDataTBL).filter_by(
            user_id=self.user_id,
            jtable_name=jtable_name,
        ).update({
            ObVecJsonTableClient.JsonTableDataTBL.jdata: func.json_insert(
                func.json_remove(
                    ObVecJsonTableClient.JsonTableDataTBL.jdata, f'$.{origin_col_name}'
                ),
                f'$.{new_col_name}',
                func.json_value(
                    ObVecJsonTableClient.JsonTableDataTBL.jdata, f'$.{origin_col_name}',
                ),
            )
        })

        session.query(ObVecJsonTableClient.JsonTableDataTBL).filter_by(
            user_id=self.user_id,
            jtable_name=jtable_name,
        ).update({
            ObVecJsonTableClient.JsonTableDataTBL.jdata: func.json_replace(
                ObVecJsonTableClient.JsonTableDataTBL.jdata,
                f'$.{new_col_name}',
                json_value(
                    ObVecJsonTableClient.JsonTableDataTBL.jdata,
                    f'$.{new_col_name}',
                    col_type_str,
                ),
            )
        })

    def _handle_alter_jtable_drop_column(
        self,
        session: Session,
        jtable_name: str,
        drop_col: Expression,
    ):
        logger.debug("HANDLE ALTER DROP COLUMN")
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

        session.query(ObVecJsonTableClient.JsonTableDataTBL).filter_by(
            user_id=self.user_id,
            jtable_name=jtable_name,
        ).update({
            ObVecJsonTableClient.JsonTableDataTBL.jdata: func.json_remove(
                ObVecJsonTableClient.JsonTableDataTBL.jdata, f'$.{col_name}'
            )
        })

    def _handle_alter_jtable_add_column(
        self,
        session: Session,
        jtable_name: str,
        add_col: Expression,
    ):
        logger.debug("HANDLE ALTER ADD COLUMN")
        new_col_name = add_col.this.this
        if self._check_col_exists(jtable_name, new_col_name):
            raise ValueError(f"{new_col_name} exists!")
        
        col_type_str = self._parse_col_datatype(add_col.kind)
        model = ObVecJsonTableClient.JsonTableMetadata._parse_col_type(col_type_str)
        constraints = self._parse_col_constraints(add_col.constraints)
        if (not constraints['jcol_nullable']) and constraints['jcol_has_default'] and (constraints['jcol_default'] is None):
            raise ValueError(f"Invalid default value for '{new_col_name}'")
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

        if constraints['jcol_default'] is None:
            session.query(ObVecJsonTableClient.JsonTableDataTBL).filter_by(
                user_id=self.user_id,
                jtable_name=jtable_name,
            ).update({
                ObVecJsonTableClient.JsonTableDataTBL.jdata: func.json_insert(
                    ObVecJsonTableClient.JsonTableDataTBL.jdata,
                    f'$.{new_col_name}',
                    None,
                )
            })
        else:
            model = ObVecJsonTableClient.JsonTableMetadata._parse_col_type(col_type_str)
            datum = model(val=self._calc_default_value(constraints['jcol_default']))
            json_val = val2json(datum.val)
            session.query(ObVecJsonTableClient.JsonTableDataTBL).filter_by(
                user_id=self.user_id,
                jtable_name=jtable_name,
            ).update({
                ObVecJsonTableClient.JsonTableDataTBL.jdata: func.json_insert(
                    ObVecJsonTableClient.JsonTableDataTBL.jdata,
                    f'$.{new_col_name}',
                    json_val,
                )
            })

    def _handle_alter_jtable_modify_column(
        self,
        session: Session,
        jtable_name: str,
        modify_col: Expression,
    ):
        logger.debug("HANDLE ALTER MODIFY COLUMN")
        col_def = modify_col.this
        col_name = col_def.this.this
        if not self._check_col_exists(jtable_name, col_name):
            raise ValueError(f"{col_name} not exists in {jtable_name}")
        
        col_type_str = self._parse_col_datatype(col_def.kind)
        model = ObVecJsonTableClient.JsonTableMetadata._parse_col_type(col_type_str)
        constraints = self._parse_col_constraints(col_def.constraints)
        if (not constraints['jcol_nullable']) and constraints['jcol_has_default'] and (constraints['jcol_default'] is None):
            raise ValueError(f"Invalid default value for '{col_name}'")
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

        if constraints['jcol_default'] is None:
            session.query(ObVecJsonTableClient.JsonTableDataTBL).filter_by(
                user_id=self.user_id,
                jtable_name=jtable_name,
            ).update({
                ObVecJsonTableClient.JsonTableDataTBL.jdata: func.json_replace(
                    ObVecJsonTableClient.JsonTableDataTBL.jdata,
                    f'$.{col_name}',
                    json_value(
                        ObVecJsonTableClient.JsonTableDataTBL.jdata,
                        f'$.{col_name}',
                        col_type_str,
                    ),
                )
            })
        else:
            model = ObVecJsonTableClient.JsonTableMetadata._parse_col_type(col_type_str)
            datum = model(val=self._calc_default_value(constraints['jcol_default']))
            json_val = val2json(datum.val)
            session.query(ObVecJsonTableClient.JsonTableDataTBL).filter_by(
                user_id=self.user_id,
                jtable_name=jtable_name,
            ).update({
                ObVecJsonTableClient.JsonTableDataTBL.jdata: func.json_replace(
                    ObVecJsonTableClient.JsonTableDataTBL.jdata,
                    f'$.{col_name}',
                    func.ifnull(
                        json_value(
                            ObVecJsonTableClient.JsonTableDataTBL.jdata,
                            f'$.{col_name}',
                            col_type_str,
                        ),
                        json_val,
                    ),
                )
            })

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
        
        session = self.session()
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
        else:
            table_name = ast.this.this.this
        if not self._check_table_exists(table_name):
            raise ValueError(f"Table {table_name} does not exists")
        
        table_col_names = [meta['jcol_name'] for meta in self.jmetadata.meta_cache[table_name]]
        cols = {
            meta['jcol_name']: meta
            for meta in self.jmetadata.meta_cache[table_name]
        }
        if isinstance(ast.this, exp.Schema):
            insert_col_names = [expr.this for expr in ast.this.expressions]
            for col_name in insert_col_names:
                if col_name not in table_col_names:
                    raise ValueError(f"Unknown column {col_name} in field list")
            for meta in self.jmetadata.meta_cache[table_name]:
                if ((meta['jcol_name'] not in insert_col_names) and
                    (not meta['jcol_nullable']) and (not meta['jcol_has_default'])):
                    raise ValueError(f"Field {meta['jcol_name']} does not have a default value")
        elif isinstance(ast.this, exp.Table):
            insert_col_names = table_col_names
        else:
            raise ValueError(f"Invalid ast type {ast.this}")

        session = self.session()
        for tuple in ast.expression.expressions:
            expr_list = tuple.expressions
            if len(expr_list) != len(insert_col_names):
                raise ValueError(f"Values Tuple length does not match with the length of insert columns")
            kv = {}
            for col_name, expr in zip(insert_col_names, expr_list):
                model = cols[col_name]['jcol_model']
                datum = model(val=self._calc_default_value(str(expr)))
                kv[col_name] = val2json(datum.val)
            for col_name in table_col_names:
                if col_name not in insert_col_names:
                    model = cols[col_name]['jcol_model']
                    datum = model(val=self._calc_default_value(cols[col_name]['jcol_default']))
                    kv[col_name] = val2json(datum.val)

            logger.debug(f"================= [INSERT] =============== {kv}")

            session.add(ObVecJsonTableClient.JsonTableDataTBL(
                user_id = self.user_id,
                jtable_name = table_name,
                jdata = kv,
            ))
        
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error occurred: {e}")
        finally:
            session.close()

    def _handle_jtable_dml_update(self, ast: Expression):
        table_name = ast.this.this.this
        if not self._check_table_exists(table_name):
            raise ValueError(f"Table {table_name} does not exists")
        
        path_settings = []
        for expr in ast.expressions:
            col_name = expr.this.this.this
            if not self._check_col_exists(table_name, col_name):
                raise ValueError(f"Column {col_name} does not exists")
            col_expr = expr.expression
            new_node = parse_one(f"JSON_VALUE({JSON_TABLE_DATA_TABLE_NAME}.jdata, '$.{col_name}')")
            for column in col_expr.find_all(exp.Column):
                parent_node = column.parent
                if isinstance(parent_node.args[column.arg_key], list):
                    new_expr_list = []
                    for node in parent_node.args[column.arg_key]:
                        if isinstance(node, exp.Column):
                            new_expr_list.append(new_node)
                        else:
                            new_expr_list.append(node)
                    parent_node.args[column.arg_key] = new_expr_list
                elif isinstance(parent_node.args[column.arg_key], exp.Column):
                    parent_node.args[column.arg_key] = new_node
            logger.info(str(col_expr))
            path_settings.append(f"'$.{col_name}', {str(col_expr)}")

        where_clause = None        
        if 'where' in ast.args.keys():
            for column in ast.args['where'].find_all(exp.Column):
                where_col_name = column.this.this
                if not self._check_col_exists(table_name, where_col_name):
                    raise ValueError(f"Column {where_col_name} does not exists")
                column.parent.args['this'] = parse_one(
                    f"JSON_VALUE({JSON_TABLE_DATA_TABLE_NAME}.jdata, '$.{where_col_name}')"
                )
            where_clause = f"{JSON_TABLE_DATA_TABLE_NAME}.user_id = '{self.user_id}' AND {JSON_TABLE_DATA_TABLE_NAME}.jtable_name = '{table_name}' AND ({str(ast.args['where'].this)})"
        
        if where_clause:
            update_sql = f"UPDATE {JSON_TABLE_DATA_TABLE_NAME} SET jdata = JSON_REPLACE({JSON_TABLE_DATA_TABLE_NAME}.jdata, {', '.join(path_settings)}) WHERE {where_clause}"
        else:
            update_sql = f"UPDATE {JSON_TABLE_DATA_TABLE_NAME} SET jdata = JSON_REPLACE({JSON_TABLE_DATA_TABLE_NAME}.jdata, {', '.join(path_settings)})"

        logger.debug(f"===================== do update: {update_sql}")
        self.perform_raw_text_sql(update_sql)

    def _handle_jtable_dml_delete(self, ast: Expression):
        table_name = ast.this.this.this
        if not self._check_table_exists(table_name):
            raise ValueError(f"Table {table_name} does not exists")
        
        where_clause = None
        if 'where' in ast.args.keys():
            for column in ast.args['where'].find_all(exp.Column):
                where_col_name = column.this.this
                if not self._check_col_exists(table_name, where_col_name):
                    raise ValueError(f"Column {where_col_name} does not exists")
                column.parent.args['this'] = parse_one(
                    f"JSON_VALUE({JSON_TABLE_DATA_TABLE_NAME}.jdata, '$.{where_col_name}')"
                )
            where_clause = f"{JSON_TABLE_DATA_TABLE_NAME}.user_id = '{self.user_id}' AND {JSON_TABLE_DATA_TABLE_NAME}.jtable_name = '{table_name}' AND ({str(ast.args['where'].this)})"
        
        if where_clause:
            delete_sql = f"DELETE FROM {JSON_TABLE_DATA_TABLE_NAME} WHERE {where_clause}"
        else:
            delete_sql = f"DELETE FROM {JSON_TABLE_DATA_TABLE_NAME}"

        logger.debug(f"===================== do delete: {delete_sql}")
        self.perform_raw_text_sql(delete_sql)

    def _get_full_datatype(self, jdata_type: str):
        if jdata_type.upper() == "VARCHAR":
            return "VARCHAR(255)"
        if jdata_type.upper() == "DECIMAL":
            return "DECIMAL(10, 0)"
        return jdata_type

    def _handle_jtable_dml_select(self, ast: Expression):
        table_name = ast.args['from'].this.this.this
        if not self._check_table_exists(table_name):
            raise ValueError(f"Table {table_name} does not exists")
        
        ast.args['from'].args['this'].args['this'] = to_identifier(name=JSON_TABLE_DATA_TABLE_NAME, quoted=False)

        col_meta = self.jmetadata.meta_cache[table_name]
        json_table_meta_str = []
        all_jcol_names = []
        for meta in col_meta:
            json_table_meta_str.append(
                f"{meta['jcol_name']} {self._get_full_datatype(meta['jcol_type'])} "
                f"PATH '$.{meta['jcol_name']}'"
            )
            all_jcol_names.append(meta['jcol_name'])
        
        need_replace_select_exprs = False
        new_select_exprs = []
        for select_expr in ast.args['expressions']:
            if isinstance(select_expr, exp.Star):
                need_replace_select_exprs = True
                for jcol_name in all_jcol_names:
                    col_expr = exp.Column()
                    identifier = exp.Identifier()
                    identifier.args['this'] = jcol_name
                    identifier.args['quoted'] = False
                    col_expr.args['this'] = identifier
                    new_select_exprs.append(col_expr)
            else:
                new_select_exprs.append(select_expr)
        if need_replace_select_exprs:
            ast.args['expressions'] = new_select_exprs
        
        tmp_table_name = "__tmp"
        json_table_str = f"json_table({JSON_TABLE_DATA_TABLE_NAME}.jdata, '$' COLUMNS ({', '.join(json_table_meta_str)})) {tmp_table_name}"

        for col in ast.find_all(exp.Column):
            if 'table' in col.args.keys():
                col.args['table'].args['this'] = tmp_table_name
            else:
                identifier = exp.Identifier()
                identifier.args['this'] = tmp_table_name
                identifier.args['quoted'] = False
                col.args['table'] = identifier

        join_clause = parse_one(f"from t1, {json_table_str}")
        join_node = join_clause.args['joins'][0]
        if 'joins' in ast.args.keys():
            ast.args['joins'].append(join_node)
        else:
            ast.args['joins'] = [join_node]

        extra_filter_str = f"{JSON_TABLE_DATA_TABLE_NAME}.user_id = '{self.user_id}' AND {JSON_TABLE_DATA_TABLE_NAME}.jtable_name = '{table_name}'"
        if 'where' in ast.args.keys():
            filter_str = str(ast.args['where'].args['this'])
            new_filter_str = f"{extra_filter_str} AND ({filter_str})"
            ast.args['where'].args['this'] = parse_one(new_filter_str)
        else:
            where_clause = exp.Where()
            where_clause.args['this'] = parse_one(extra_filter_str)
            ast.args['where'] = where_clause

        select_sql = str(ast)
        logger.debug(f"===================== do select: {select_sql}")
        return self.perform_raw_text_sql(select_sql)
