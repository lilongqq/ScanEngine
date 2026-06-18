#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import shelve
import logging
import traceback
from collections.abc import Mapping
from typing import Optional
from common import classmap
from common.functools import get_class
from functools import partial
from crawling.database.dbproxy.dbpcrawler import DBProxyBatch, DBProxyTools, DBProxyError, FormatPath
from common.globals import Variables
from crawling.interface import Iterator


class QueryAppendIterator(Iterator):

    def __init__(self,
                 identity,
                 rules,
                 auth,
                 schema_name,
                 table_name,
                 query_columns,
                 append_column,
                 order_column: Optional[Mapping] = None
                 ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.client = DBProxyBatch(**auth)
        self.identity = identity
        self.rules = rules
        self.schema_name = schema_name
        self.table_name = table_name
        self.query_columns = query_columns
        self.append_column = append_column
        self.append_column_index = query_columns.index(self.append_column)
        self.order_column = order_column
        self.iterator = get_class(classmap.iterator.get(append_column['cls']))(append_column['auth'])
        self.fields = [item['name'] for item in self.query_columns]
        self.start = 0
        self.limit = 100
        self.fetch_size = 1000
        self.node = dict()
        self.stack = list()
        self.format = self.gen_format()
        self.__dict__.update(self.variables.dbp['string'])
        self.strategy_dir = os.path.join(
            self.variables.strategy,
            self.identity,
            self.rules,
            self.schema_name,
            self.table_name
        )
        if not os.path.exists(self.strategy_dir):
            os.makedirs(self.strategy_dir)
        self.record_shelve = shelve.open(os.path.join(self.strategy_dir, 'record.shelve'), flag='cfu')
        self.where = self.gen_condition()

    def gen_condition(self):
        if self.order_column:
            if 'row' in self.record_shelve:
                row = self.record_shelve['row']
                index = self.query_columns.index(self.order_column)
                value = row[index]
                where = 'WHERE {column_name} > \'{value}\' ORDER BY {column_name}'.format(
                    column_name=self.order_column['name'],
                    value=value)
            else:
                where = 'ORDER BY {column_name}'.format(
                    column_name=self.order_column['name']
                )
            return where
        else:
            return None

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.iterator:
                try:
                    node, stream = next(self.iterator)
                    node.update(self.node)
                except StopIteration:
                    self.logger.info('Unexpected Exception')
                else:
                    return node, stream
            elif len(self.stack) > 0:
                self.node = self.stack.pop()
                try:
                    nodes = self.iterator.get_nodes(self.node['format_path'])
                    nodes.reverse()
                    self.iterator.add_nodes(nodes)
                except:
                    self.logger.error(traceback.format_exc())
            else:
                nodes = list()
                data = self.client.query(
                    self.schema_name,
                    self.table_name,
                    self.fetch_size,
                    self.start,
                    self.limit,
                    *self.fields,
                    where=self.where
                )
                rows = data.rows
                count = len(rows)
                self.start += self.limit
                if count > 0:
                    for row in rows:
                        path = row[self.append_column_index].strip()
                        if path and path not in {'/', '', '\\'}:
                            node = dict()
                            node['schemaName'] = self.schema_name
                            node['tableName'] = self.table_name
                            node['columns'] = self.query_columns
                            node['rows'] = [row]
                            node['format_path'] = self.format(path)
                            node.update(zip(self.fields, row))
                            nodes.append(node)
                    else:
                        self.record_shelve['row'] = row
                    nodes.reverse()
                    self.stack.extend(nodes)
                else:
                    raise StopIteration

    def __del__(self):
        if getattr(self, 'record_shelve', False):
            self.record_shelve.close()

    @staticmethod
    def format_path(formats, path):
        for fun in formats:
            path = fun(path)
        else:
            return path

    def gen_format(self):
        formats = list()
        formats.append(FormatPath.windows)
        if 'format' in self.append_column:
            for k, v in self.append_column['format'].items():
                formats.append(
                    partial(getattr(FormatPath, k), FormatPath.windows(v))
                )
        return partial(self.format_path, formats)


class ShanxiOAIterator(Iterator):

    def __init__(self, auth, resources, identity, rules):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.identity = identity
        self.rules = rules
        self.auth = auth
        self.client = DBProxyBatch(**self.auth)
        self.stack = list()
        self.node = dict()
        self.stack.append(resources)
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
                except DBProxyError:
                    self.logger.error(traceback.format_exc())
                    self.iterator = None
                else:
                    return node, stream
            if len(self.iterators) > 0:
                self.iterator = self.iterators.pop()
            else:
                while len(self.stack) > 0:
                    self.node = self.stack.pop()
                    if 'layer' in self.node and self.node['layer'] == 'table':
                        iterators = self.get_iterators(self.node)
                        iterators.reverse()
                        self.iterators.extend(iterators)
                        break
                    elif 'children' in self.node:
                        children = self.node['children']
                        children.reverse()
                        self.stack.extend(children)
                else:
                    raise StopIteration

    def get_iterators(self, node):
        iterators = list()
        columns = node['children']
        append_columns = DBProxyTools.get_append_columns(columns)
        order_column = DBProxyTools.get_order_column(columns)
        query_columns = DBProxyTools.get_query_columns(columns)
        for append_column in append_columns:
            kwargs = {
                'identity': self.identity,
                'rules': self.rules,
                'auth': self.auth,
                'schema_name': node['schema_name'],
                'table_name': node['table_name'],
                'append_column': append_column,
                'query_columns': query_columns
            }
            if order_column:
                kwargs['order_column'] = order_column
            iterators.append(
                QueryAppendIterator(**kwargs)
            )
        return iterators
