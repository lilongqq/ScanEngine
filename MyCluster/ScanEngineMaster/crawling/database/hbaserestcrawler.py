#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import logging
from common.globals import Variables
import json
import traceback
from collections import UserDict
import hashlib
import os.path
from collections.abc import MutableMapping
from functools import partial, singledispatchmethod
from common.functools import gen_key
from crawling.interface import Iterator
from hbase.rest_client import HBaseRESTClient
from hbase.admin import HBaseAdmin
from hbase.get import Get
from hbase.scan_filter_helper import build_base_scanner
from ex3rd.hbase.scan import MyScan
from exceptions import HBASERESTError


class HBaseTools(object):

    @staticmethod
    def get_schema_table_name(path):
        path_tuple = path.split('/', 1)
        if len(path_tuple) == 1:
            schema_name, = path_tuple
            table_name = ''
            return schema_name, table_name
        elif len(path_tuple) == 2:
            schema_name, table_name = path_tuple
            return schema_name, table_name


class HBaseDict(UserDict):
    
    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['last_write_time']
        elif item == 'diff':
            return gen_key(
                name=self.data.get('name'),
                path=self.data.get('path')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return {'name', 'path'}.issubset(self.data)
        else:
            return item in self.data


class HBaseRest(object):

    def __init__(self, url):
        self.client = HBaseRESTClient([url])
        self.admin = HBaseAdmin(self.client)
        self.scan = MyScan(self.client)
        self.get = Get(self.client)
        self.version = self.cluster_version()

    def cluster_version(self):
        resp = self.admin.cluster_version()
        if isinstance(resp, dict):
            raise HBASERESTError(resp)
        else:
            result = json.loads(resp)
            if isinstance(result, str):
                return result
            else:
                return result['Version']

    @property
    def db_info(self):
        return {
            'message': '',
            'productName': 'hbase',
            'productVersion': self.version,
            'readable': 2
        }

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        if 'layer' in node:
            if node['layer'] == 'schema':
                return self.get_tables(node)
            elif node['layer'] == 'table':
                return self.get_columns(node)
            else:
                raise ValueError('layer is illegal')
        else:
            path = node.get('path', node['name'])
            if path:
                schema_name, table_name = HBaseTools.get_schema_table_name(path)
                if table_name:
                    node['schema_name'] = schema_name
                    node['table_name'] = table_name
                    return self.get_columns(node)
                else:
                    node['schema_name'] = schema_name
                    return self.get_tables(node)
            else:
                return self.get_namespaces(node)

    @get_nodes.register(str)
    def _(self, path):
        node = {}
        if path:
            schema_name, table_name = HBaseTools.get_schema_table_name(path)
            if table_name:
                node['schema_name'] = schema_name
                node['table_name'] = table_name
                return self.get_columns(node)
            else:
                node['schema_name'] = schema_name
                return self.get_tables(node)
        else:
            return self.get_namespaces(node)

    def get_file(self, node):
        schema_name = node['schemaName']
        table_name = node['tableName']
        row_key = node['rows'][0][0]
        # timestamp = node['rows'][0][1]
        column_family = node['columns'][1]['column_family']
        resp = self.get.get(
            '{}:{}'.format(schema_name, table_name),
            row_key=row_key,
            column_family=column_family,
            # timestamp=timestamp
        )
        row_list = resp['row']
        for row in row_list:
            cell_list = row['cell']
            for cell in cell_list:
                column_name = str(cell['column'], encoding='utf-8')
                timestamp = cell['timestamp']
                data = cell['$']
                if node['columns'][1]['name'] == column_name and int(node['rows'][0][1]) == timestamp:
                    return io.BytesIO(data)

    def get_namespaces(self, root):
        nodes = list()
        resp = self.admin.namespaces()
        if isinstance(resp, dict):
            raise HBASERESTError(resp)
        else:
            namespaces = json.loads(resp)
            for name in namespaces['Namespace']:
                node = dict()
                node['name'] = name
                node['path'] = name
                node['schema_name'] = name
                node['layer'] = 'schema'
                node['children'] = []
                if 'total' in root:
                    node['total'] = root['total']
                if name == 'hbase':
                    node['schema_def'] = 'systemDef'
                else:
                    node['schema_def'] = 'userDef'
                nodes.append(node)
            return nodes

    def get_tables(self, schema):
        nodes = list()
        schema_name = schema['schema_name']
        resp = self.admin.tables(schema_name)
        if isinstance(resp, dict):
            raise HBASERESTError(resp)
        else:
            tables = json.loads(resp)
            for table in tables['table']:
                node = dict()
                node['name'] = table['name']
                node['path'] = os.path.join(schema_name, table['name'])
                node['table_name'] = table['name']
                node['schema_name'] = schema_name
                node['layer'] = 'table'
                node['children'] = []
                if 'total' in schema:
                    node['total'] = schema['total']
                nodes.append(node)
            return nodes

    def get_columns(self, table):
        nodes = list()
        schema_name = table['schema_name']
        table_name = table['table_name']
        resp = self.admin.table_schema('{}:{}'.format(schema_name, table_name))
        if isinstance(resp, dict):
            raise HBASERESTError(resp)
        else:
            column_schemas = json.loads(resp)
            for column in column_schemas['ColumnSchema']:
                column['layer'] = 'column'
                column['classification'] = 'BINARY'  # 'STRING'
                column['typeName'] = 'BLOB'  # 'TEXT'
                column['column_family'] = column['name']
                nodes.append(column)
            return nodes

    def create_scanner(self, table_name, limit, column_name):
        resp = self.scan.create_scanner(
            table_name,
            build_base_scanner(batch=limit, column=[column_name])
        )
        if isinstance(resp, dict):
            raise HBASERESTError(resp)
        else:
            return resp

    def scan_next(self):
        resp = self.scan.scan_next()
        return resp

    def __del__(self):
        self.client.session.close()


class BinaryIterator(Iterator):

    def __init__(self,
                 auth,
                 schema_name,
                 table_name,
                 column,
                 total=0
                 ):
        self.variables = Variables()
        self.auth = auth
        self.client = HBaseRest(**self.auth)
        self.schema_name = schema_name
        self.table_name = table_name
        self.index_column = {
            'name': 'rowkey',
            'layer': 'column',
            'classification': 'STRING',
            'typeName': 'TEXT',
            'pkColumn': 'YES'
        }
        self.column = column
        self.start = 0
        self.limit = 10
        self.count = 0
        self.total = total
        self.stack = list()
        self.__dict__.update(self.variables.hbase)
        self.client.create_scanner(self.table_name, self.limit, self.column['name'])

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if len(self.stack) > 0:
                self.node, data = self.stack.pop()
                self.node['auth'] = self.auth
                self.node['cls'] = 'HBASEREST'
                return self.node, partial(self.wrapper_binary, self.node, data)
            else:
                if 0 < self.total <= self.start:
                    raise StopIteration
                else:
                    is_truncated, resp = self.client.scan_next()
                    self.start += self.limit
                    if is_truncated:
                        row_list = resp['row']
                        count = len(row_list)
                        self.count += count
                        if 0 < self.total < self.count:
                            row_list = row_list[:(self.count - self.total)]
                        for row in row_list:
                            rowkey = str(row['key'], encoding='utf-8')
                            cell_list = row['cell']
                            for cell in cell_list:
                                column_name = str(cell['column'], encoding='utf-8')
                                timestamp = cell['timestamp']
                                data = cell['$']
                                column = dict(**self.column)
                                column['name'] = column_name
                                node = HBaseDict()
                                rows = list()
                                rows.append(
                                    [rowkey, timestamp]
                                )
                                node['size'] = len(data)
                                node['rows'] = rows
                                node['name'] = os.path.join(
                                    'rowkey-{rowkey}'.format(
                                        rowkey=rowkey
                                    ),
                                    '{column_name}-{index}.BLOB'.format(
                                        column_name=column_name,
                                        index=timestamp
                                    )
                                )
                                node['path'] = os.path.join(
                                    self.schema_name,
                                    self.table_name
                                )
                                node['type'] = 'binary'
                                node['schemaName'] = self.schema_name
                                node['tableName'] = self.table_name
                                node['columns'] = [self.index_column, column]
                                node['last_write_time'] = timestamp / 1000
                                self.stack.append((node, data))
                        self.stack.reverse()
                    else:
                        raise StopIteration

    @staticmethod
    def wrapper_binary(node, data):
        md5 = hashlib.md5()
        md5.update(data)
        node['md5'] = md5.hexdigest()
        return data


class HBaseIterator(Iterator):

    def __init__(self, auth, resources):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.client = HBaseRest(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = dict()
        self.iterators = list()
        self.iterator = None

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.iterator:
                try:
                    node, stream = next(self.iterator)
                except StopIteration:
                    self.iterator = None
                except OSError:
                    self.logger.error(traceback.format_exc())
                    self.iterator = None
                except Exception:
                    self.logger.error(traceback.format_exc())
                    self.iterator = None
                else:
                    return node, stream
            if len(self.iterators) > 0:
                self.iterator = self.iterators.pop()
            else:
                while len(self.stack) > 0:
                    self.node = HBaseDict(self.stack.pop())
                    if 'layer' in self.node and self.node['layer'] == 'table':
                        iterators = self.get_iterators(self.node)
                        iterators.reverse()
                        self.iterators.extend(iterators)
                        break
                    elif 'children' in self.node:
                        if self.node['children']:
                            children = self.node['children']
                        else:
                            children = self.get_nodes(self.node)
                        children.reverse()
                        self.stack.extend(children)
                else:
                    raise StopIteration

    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    def get_iterators(self, node):
        iterators = list()
        if node['children']:
            columns = node['children']
        else:
            columns = self.get_nodes(node)
        for column in columns:
            kwargs = {
                'auth': self.auth,
                'schema_name': node['schema_name'],
                'table_name': node['table_name'],
                'column': column
            }
            if 'total' in node:
                kwargs['total'] = node['total']
            iterators.append(
                BinaryIterator(**kwargs)
            )
        return iterators
