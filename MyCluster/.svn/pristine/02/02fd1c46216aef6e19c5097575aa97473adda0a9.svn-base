import logging
import os.path
import json
import hashlib
import traceback
import io
from collections import UserDict
from functools import partial, cached_property, singledispatchmethod
from collections.abc import MutableMapping
from pymongo import MongoClient
from bson.json_util import dumps
from bson import ObjectId
from gridfs import GridFS
from crawling.interface import Iterator, Client
from common.conntool import weak_cache
from common.functools import gen_key


class QueryDict(UserDict):

    @cached_property
    def value(self):
        return json.dumps(self.data, ensure_ascii=False).encode('utf-8')

    def __len__(self):
        return len(self.value)


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
                return ValueError('layer is illegal')
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
                        v = dumps(record)
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
                if 'children' in self.node:
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
                    try:
                        self.cursor = self.client.get_cursor(self.node)
                        self.start = 0
                    except:
                        self.logger.error(traceback.format_exc())
                        continue
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

    def get_nodes(self, node):
        nodes = list()
        if 'layer' in node:
            if node['layer'] == 'schema':
                database = self.client.get_database(node['name'])
                names = database.list_collection_names()
                for name in names:
                    if name.endswith('.files'):
                        collection = dict()
                        node['type'] = 'file'
                        collection['name'] = name.removesuffix('.files')
                        collection['path'] = os.path.join(
                            node['path'],
                            collection['name']
                        )
                        collection['schema_name'] = node['name']
                        collection['layer'] = 'table'
                        nodes.append(collection)
            elif node['layer'] == 'table':
                return ValueError('layer is illegal')
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
        f = io.BytesIO()
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
                    node['name'] = node['file_id']
                    node['path'] = os.path.join(
                        self.node['path'],
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
        f = io.BytesIO()
        while True:
            chunk = grid_out.readchunk()
            if not len(chunk):
                break
            f.write(chunk)
        f.seek(0)
        if not node['md5']:
            md5 = hashlib.md5()
            md5.update(f.getbuffer())
            node['md5'] = md5.hexdigest()
        return f.getvalue()
