#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os.path
import hashlib
import traceback
from tempfile import SpooledTemporaryFile
from collections import UserDict
from functools import partial, cached_property, singledispatchmethod
from collections.abc import MutableMapping
from pymongo import MongoClient
from bson import ObjectId
from gridfs import GridFS
from crawling.interface import Iterator, Client
from common.functools import gen_key


class GRIDFSDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['create_time']
        elif item == 'diff':
            return gen_key(
                path=self.data.get('path'),
                last_write_time=self.data.get('create_time')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'create_time' in self.data
        elif item == 'diff':
            return {'create_time', 'path'}.issubset(self.data)
        else:
            return item in self.data


class GRIDFS(Client):

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
            authSource=self.authSource
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

    @staticmethod
    def __database_and_collection(path):
        path_tuple = path.split('/', 1)
        if len(path_tuple) == 2:
            database_name, collection_name = path_tuple
        else:
            database_name = path
            collection_name = ''
        return database_name, collection_name

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

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
                        collection = dict()
                        collection['type'] = 'file'
                        collection['name'] = name.removesuffix('.files')
                        collection['path'] = os.path.join(
                            database_name,
                            collection['name']
                        )
                        collection['schema_name'] = database_name
                        collection['layer'] = 'table'
                        nodes.append(collection)
        else:
            names = self.client.list_database_names()
            for name in names:
                node = dict()
                node['type'] = 'file'
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
                        collection = dict()
                        collection['type'] = 'file'
                        collection['name'] = name.removesuffix('.files')
                        collection['path'] = os.path.join(
                            node['path'],
                            collection['name']
                        )
                        collection['schema_name'] = node['schema_name']
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
                node['type'] = 'file'
                node['name'] = name
                node['path'] = name
                node['layer'] = 'schema'
                node['schema_name'] = name
                node['children'] = []
                node['schema_def'] = 'userDef'
                nodes.append(node)
        return nodes

    def get_file(self, node):
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        database = self.client.get_database(node['schema_name'])
        fs = GridFS(database, node['bucket_name'])
        grid_out = fs.get(ObjectId(node['file_id']))
        while True:
            chunk = grid_out.readchunk()
            if not len(chunk):
                break
            f.write(chunk)
        f.seek(0)
        return f

    def get_cursor(self, node):
        database = self.client.get_database(node['schema_name'])
        fs = GridFS(database, node['name'])
        return fs.find()

    def __bool__(self):
        return True

    def __del__(self):
        if getattr(self, 'client', False):
            self.client.close()


class GRIDFSIterator(Iterator):

    def __init__(self, auth, resources):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.cursor = None
        self.client = GRIDFS(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = GRIDFSDict()

    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.cursor:
                try:
                    grid_out = next(self.cursor)
                    node = GRIDFSDict()
                    node['type'] = 'file'
                    node['auth'] = self.auth
                    node['cls'] = 'GRIDFS'
                    node['schema_name'] = self.node['schema_name']
                    node['bucket_name'] = self.node['name']
                    node['file_id'] = str(grid_out._id)
                    if grid_out.filename:
                        node['name'] = grid_out.filename
                    else:
                        node['name'] = 'unknown'
                    node['path'] = os.path.join(
                        self.node['path'],
                        node['file_id'],
                        node['name']
                    )
                    node['size'] = grid_out.length
                    node['create_time'] = grid_out.upload_date.timestamp()
                    node['md5'] = getattr(grid_out, 'md5')
                    if grid_out.metadata:
                        node.update(grid_out.metadata)
                    return node, partial(self.get_file, node, grid_out)
                except StopIteration:
                    self.cursor = None
                except:
                    self.logger.error(traceback.format_exc())
                    self.cursor = None
            elif len(self.stack) > 0:
                self.node = GRIDFSDict(self.stack.pop())
                if 'children' in self.node:
                    if self.node['children']:
                        children = self.node['children']
                    else:
                        children = self.get_nodes(self.node)
                    children.reverse()
                    self.stack.extend(children)
                else:
                    self.cursor = self.client.get_cursor(self.node)
            else:
                raise StopIteration

    def get_file(self, node, grid_out):
        md5 = hashlib.md5()
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        while True:
            chunk = grid_out.readchunk()
            if not len(chunk):
                break
            f.write(chunk)
            if not node['md5']:
                md5.update(chunk)
        if not node['md5']:
            node['md5'] = md5.hexdigest()
        f.seek(0)
        return f
