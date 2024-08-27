# -*- coding: utf-8 -*-
"""
@Time    : 2022/8/18 23:42
@Author  : itlubber
@Site    : itlubber.art
"""
import warnings

warnings.filterwarnings("ignore")

import pymysql
import traceback
import pandas as pd
from tqdm import tqdm
from clickhouse_driver import dbapi
from contextlib import contextmanager

from .pooled_db import PooledDB


class MysqlConnectPoolQuery:

    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor

    def close(self):
        self.conn.close()
        self.cursor.close()


class MysqlConnectPool:

    def __init__(self, creator=pymysql, **kwargs):
        self._pool = PooledDB(creator, **kwargs)

    @contextmanager
    def register_connect_query(self):
        try:
            _conn = self._pool.connection()
            _cursor = _conn.cursor()
            _query = MysqlConnectPoolQuery(_conn, _cursor)
            yield _query
        except Exception as error:
            print(traceback.format_exc())
        finally:
            _query.close()

    def query(self, query, result="df", index=None):
        with self.register_connect_query() as _connect_query:
            if result == "df":
                return pd.read_sql_query(query, _connect_query.conn)
            else:
                count = _connect_query.cursor.execute(query)
                if count > 0:
                    if index:
                        _data = as_pandas(_connect_query.cursor)
                        if index not in _data.columns:
                            raise "index must in the result set's schema."
                        else:
                            if _data[index].nunique() == len(_data) and (result is None or result != "record"):
                                _data.set_index(index).to_dict(orient="index")
                            else:
                                _result = {group_name: group.drop(columns=[index]).to_dict(orient="record") for group_name, group in _data.groupby(index)}
                                return _result
                
                    if result == "record":
                        column_names = [col[0] for col in _connect_query.cursor.description]
                        return [dict(zip(column_names, row)) for row in _connect_query.cursor.fetchall()]
                    else:
                        return _connect_query.cursor.fetchall()
                else:
                    return False

    def execute(self, query, params=None):
        with self.register_connect_query() as _connect_query:
            try:
                if params:
                    _connect_query.cursor.execute(query, params)
                else:
                    _connect_query.cursor.execute(query)
                _connect_query.conn.commit()
            except Exception as error:
                _connect_query.conn.rollback()
                traceback.print_exc()
                raise Exception("sql语句执行失败")
    
    def executemany(self, query, values):
        with self.register_connect_query() as _connect_query:
            try:
                _connect_query.cursor.executemany(query, values)
                _connect_query.conn.commit()
            except Exception as error:
                _connect_query.conn.rollback()
                traceback.print_exc()
                raise Exception("sql语句执行失败")

    def generate_create_query(self, df, table_name, engine='mysql', debug=False, feature_map={}, comment=None, primary_key=None):
        feature_map = {k: v for k, v in feature_map.items() if v}
        if engine == 'oracle':
            create_sql = f'CREATE TABLE {table_name} (\n'
            for column in df.columns:
                column_type = df[column].dtype.name
                if column_type in ['object', 'str']:
                    column_type = 'VARCHAR2(255)'
                elif column_type in ('int64', 'int32', 'int16', 'int8', 'float64', 'float32', 'float16'):
                    column_type = 'BIGINT'
                elif column_type == 'datetime64[ns]':
                    column_type = 'DATE'
                
                create_sql += f'    {column} {column_type},\n'

            create_sql = create_sql[:-2] + '\n)'

        elif engine == 'mysql':
            create_sql = f'CREATE TABLE IF NOT exists {table_name} (\n'
            for column in df.columns:
                column_type = df[column].dtype.name
                if column_type in ['object', 'str', 'category']:
                    column_type = 'VARCHAR(255)'
                elif column_type in ('int64', 'int32', 'int16', 'int8'):
                    if max(abs(df[column].max()), abs(df[column].max())) < 2 ** 32 / 2 - 1:
                        column_type = 'INT'
                    else:
                        column_type = 'BIGINT'
                elif column_type in ('float64', 'float32', 'float16'):
                    column_type = 'DOUBLE'
                elif column_type == 'datetime64[ns]':
                    column_type = 'DATETIME'
                
                if column in feature_map:
                    comment = f" COMMENT '{feature_map[column]}'"
                else:
                    comment = ""
                
                if primary_key is not None and column == primary_key:
                    primary = f" PRIMARY KEY"
                else:
                    primary = ""
                
                create_sql += f'    {column} {column_type}{primary}{comment},\n'

            create_sql = create_sql[:-2] + '\n)'
        
        elif engine == 'clickhouse':
            create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} (\n'
            for column in df.columns:
                column_type = df[column].dtype.name
                if column_type in ['object', 'str', 'category']:
                    column_type = 'String' if primary_key is not None and column == primary_key else 'Nullable(String)'
                elif column_type in ('int64', 'int32', 'int16', 'int8'):
                    column_type = 'Int32' if primary_key is not None and column == primary_key else 'Nullable(Int32)'
                elif column_type in ('float64', 'float32', 'float16'):
                    column_type = 'Float32' if primary_key is not None and column == primary_key else 'Nullable(Float32)'
                elif column_type == 'datetime64[ns]':
                    column_type = 'Datetime' if primary_key is not None and column == primary_key else 'Nullable(Datetime)'
                
                if column in feature_map:
                    _comment = f" COMMENT '{feature_map[column]}'"
                else:
                    _comment = ""
                
                create_sql += f'    {column} {column_type}{_comment},\n'

            create_sql = create_sql[:-2] + '\n)\nENGINE = ReplacingMergeTree()'
            
            if primary_key is not None and column in df.columns:
                create_sql += f"\nPRIMARY KEY {primary_key}"
                create_sql += f"\nORDER BY {primary_key}"
            
            if comment:
                create_sql += f"\nCOMMENT '{comment}'"
        
        else:
            raise f"不支持自动创建 {engine} 建表语句"
        
        if debug:
            return create_sql
        else:
            with self.register_connect_query() as _connect_query:
                try:
                    _connect_query.cursor.execute(create_sql)
                    _connect_query.conn.commit()
                except Exception as error:
                    _connect_query.conn.rollback()
                    traceback.print_exc()
                    raise Exception("sql语句执行失败")

    def generate_insert_query(self, df, table_name, batch_size=256, engine="mysql", debug=False, bar=False):
        data = df.copy()
        
        fields = str(df.columns.tolist())[1:-1].replace("'", "`")
        
        if engine == "oracle":
            insert_sql = f"INSERT INTO {table_name} \n({', '.join(list(data.columns))}) \nVALUES (:{', :'.join(list(data.columns))})"
        elif engine == "mysql":
            insert_sql = f"REPLACE INTO {table_name} \n({', '.join(list(data.columns))}) \nVALUES ({', '.join(['%s'] * len(data.columns))})"
        elif engine == "clickhouse":
            insert_sql = f"INSERT INTO {table_name} ({fields}) VALUES"
        else:
            raise ValueError("")

        if debug:
            return insert_sql

        with self.register_connect_query() as _connect_query:
            if bar:
                _iter = tqdm(range(0, len(data), batch_size))
            else:
                _iter = range(0, len(data), batch_size)
            for i in _iter:
                try:
                    if engine == "clickhouse":
                        _connect_query.cursor.executemany(insert_sql, [tuple(r) for r in data.iloc[i:i+batch_size].values.tolist()])
                    else:
                        _connect_query.cursor.executemany(insert_sql, [[None if pd.isnull(i) else i for i in r] for r in data.iloc[i:i+batch_size].values.tolist()])
                    _connect_query.conn.commit()
                except Exception as error:
                    _connect_query.conn.rollback()
                    traceback.print_exc()
                    raise Exception(f"sql语句执行失败: {insert_sql}")
