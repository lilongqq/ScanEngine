#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os.path
import hashlib
import traceback
from collections import UserDict
from functools import partial, cached_property, singledispatchmethod
from collections.abc import MutableMapping
from pymongo import MongoClient
from bson.json_util import dumps
from crawling.interface import Iterator, Client
from common.conntool import weak_cache


class QueryDict(UserDict):

    @cached_property
    def value(self):
        return dumps(self.data, ensure_ascii=False).encode('utf-8')

    def __len__(self):
        return len(self.value)


class MongoDict(UserDict):

    def __getitem__(self, item):
        if item == 'diff':
            return self.data.get('md5')
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'diff':
            return 'md5' in self.data
        else:
            return item in self.data


class Mongo(Client):

    def __init__(
        self,
        host,
        port,
        username='',
        password='',
        authSource='admin'
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.host = host
        self.port = int(port)
        self.username = username
        self.password = password
        self.authSource = authSource
        self.client = MongoClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            authSource=self.authSource,
            directConnection=True
        )

    @cached_property
    def db_info(self):
        info = self.client.server_info()
        return {
            'message': '',
            'productName': 'MongoDB',
            'productVersion': info.get('version'),
            'readable': 2
        }

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @staticmethod
    def __database_and_collection(path):
        path_tuple = path.split('/', 1)
        if len(path_tuple) == 2:
            database_name, collection_name = path_tuple
        else:
            database_name = path
            collection_name = ''
        return database_name, collection_name

    @get_nodes.register(str)
    def _(self, path):
        nodes = list()
        if path:
            database_name, collection_name = self.__database_and_collection(
                path
            )
            if collection_name:
                return nodes
            else:
                database = self.client.get_database(database_name)
                names = database.list_collection_names()
                for name in names:
                    if name.endswith('.files'):
                        continue
                    if name.endswith('.chunks'):
                        continue
                    node = dict()
                    node['name'] = name
                    node['path'] = os.path.join(database_name, name)
                    node['table_name'] = name
                    node['schema_name'] = database_name
                    node['layer'] = 'table'
                    nodes.append(node)
        else:
            names = self.client.list_database_names()
            for name in names:
                node = dict()
                node['name'] = name
                node['path'] = name
                node['layer'] = 'schema'
                node['schema_name'] = name
                node['children'] = []
                node['schema_def'] = 'userDef'
                nodes.append(node)
        return nodes

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if 'layer' in node:
            if node['layer'] == 'schema':
                database = self.client.get_database(node['name'])
                names = database.list_collection_names()
                for name in names:
                    if name.endswith('.files'):
                        continue
                    if name.endswith('.chunks'):
                        continue
                    collection = dict()
                    collection['name'] = name
                    collection['path'] = os.path.join(node['name'], name)
                    collection['table_name'] = name
                    collection['schema_name'] = node['name']
                    collection['layer'] = 'table'
                    nodes.append(collection)
            elif node['layer'] == 'table':
                raise ValueError('layer is illegal')
            else:
                raise ValueError('layer is illegal')
        else:
            names = self.client.list_database_names()
            for name in names:
                node = dict()
                node['name'] = name
                node['path'] = name
                node['layer'] = 'schema'
                node['schema_name'] = name
                node['children'] = []
                node['schema_def'] = 'userDef'
                nodes.append(node)
        return nodes

    def get_cursor(self, node):
        database = self.client.get_database(node['schema_name'])
        collection = database.get_collection(node['table_name'])
        return collection.find()

    def __bool__(self):
        return True

    def __del__(self):
        if getattr(self, 'client', False):
            self.client.close()


@weak_cache
class MongoOne(Mongo):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MongoIterator(Iterator):

    def __init__(self, auth, resources):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.client = Mongo(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.start = 0
        self.index_column = {
            'name': '_id',
            'layer': 'column',
            'classification': 'STRING',
            'typeName': 'TEXT',
            'pkColumn': 'YES'
        }
        self.binary_column = {
            'name': '',
            'layer': 'column',
            'classification': 'BINARY',
            'typeName': 'BLOB',
            'pkColumn': 'NO'
        }
        self.column = {
            'name': 'bson',
            'layer': 'column',
            'classification': 'STRING',
            'typeName': 'TEXT',
            'pkColumn': 'NO'
        }
        self.columns = [self.index_column, self.column]
        self.cursor = None
        self.node = {}

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.cursor:
                rows = list()
                for count in range(1000):
                    try:
                        record = next(self.cursor)
                        k = str(record.pop('_id'))
                        v = dumps(record, ensure_ascii=False)
                        rows.append([k, v])
                    except StopIteration:
                        self.cursor = None
                        break
                    except:
                        self.logger.error(traceback.format_exc())
                        self.cursor = None
                        break
                else:
                    count += 1
                if count:
                    node = MongoDict(self.node)
                    node['type'] = 'string'
                    node['columns'] = self.columns
                    node['start'] = self.start
                    node['count'] = count
                    node['name'] = '{}.rows'.format(self.start)
                    self.start += count
                    return node, partial(self.wrapper_rows, node, rows)
            elif len(self.stack) > 0:
                self.node = self.stack.pop()
                if self.node.get('layer', '') == 'table':
                    try:
                        self.cursor = self.client.get_cursor(self.node)
                        self.start = 0
                    except:
                        self.logger.error(traceback.format_exc())
                        continue
                else:
                    if self.node['children']:
                        children = self.node['children']
                    else:
                        try:
                            children = self.get_nodes(self.node)
                        except:
                            self.logger.error(traceback.format_exc())
                            continue
                    children.reverse()
                    self.stack.extend(children)
            else:
                raise StopIteration
    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    @staticmethod
    def wrapper_rows(node, rows):
        rows = QueryDict(rows=rows)
        node['size'] = len(rows.value)
        md5 = hashlib.md5()
        md5.update(rows.value)
        node['md5'] = md5.hexdigest()
        return rows
