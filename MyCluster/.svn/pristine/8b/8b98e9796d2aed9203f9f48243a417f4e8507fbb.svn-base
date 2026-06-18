# coding=utf-8

import json
import time
import traceback
import os
import redis
import hashlib
import logging
from functools import partial, cached_property
from collections import UserDict
from common.conntool import alive_check, weak_cache


class Dict(UserDict):

    @property
    def timestamp(self):
        return time.time()

    @property
    def key(self):
        return self.data.get('path')

    def __eq__(self, other):
        if self.data.get('md5') == other.get('md5'):
            return True
        else:
            return False


class QueryDict(UserDict):

    @cached_property
    def value(self):
        return json.dumps(self.data, ensure_ascii=False).encode('utf-8')

    def __len__(self):
        return len(self.value)


def gen_send_data(data_iter, limit):
    results = []
    while 1:
        try:
            data = next(data_iter)
            if isinstance(data, tuple):
                _, value = data
                format_value = [d for d in data]
            else:
                format_value = [data]
            if len(results) < limit:
                results.append(format_value)
            else:
                yield results.copy()
                results.clear()
                results.append(format_value)
        except StopIteration:
            yield results.copy()
            break


class RedisClient(object):
    def __init__(self, ip, port, password, db_index=0, max_chunk_size=1000, limit=2):
        self.ip = ip
        self.port = port
        self.password = password
        self.db_index = db_index
        pool = redis.ConnectionPool(
            host=self.ip, port=self.port, db=self.db_index, password=self.password)
        self.client = redis.Redis(connection_pool=pool)
        self.max_chunk_size = max_chunk_size
        self.limit = limit

    @cached_property
    def db_info(self):
        info = self.client.info()
        return {
            'message': '',
            'productName': 'redis',
            'productVersion': info.get('redis_version'),
            'readable': 2
        }

    def get_nodes(self, path=''):
        result = self.client.ping()
        if result:
            ver_dict = self.client.info()

        return [{'name': item, 'layer': 'schema', "children": [], "schema_def": "userDef"} for item in ver_dict if
                item.startswith('db')]


@weak_cache
class RedisOne(RedisClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@alive_check
class RedisBatch(RedisClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.clients = dict()

    def init_client(self, node):
        self.logger.info('init_client:')
        self.logger.info(node)
        node_name = node.get('name', None)
        if not node_name:
            return
        if node_name in self.clients:
            return self.clients[node_name]
        pool = redis.ConnectionPool(host=self.ip,
                                    port=self.port,
                                    db=int(node_name.split('db')[1]),
                                    password=self.password)
        client = redis.Redis(connection_pool=pool,
                             decode_responses=True
                             )
        client.db_name = node_name
        self.clients[node_name] = client
        return client

    def keys_iter(self, client):
        curs = client.scan(cursor=0, count=1000)
        while True:
            for key in curs[1]:
                # bytes to str
                yield key, client.type(key)
            if not curs[0]:
                break
            curs = client.scan(cursor=curs[0], count=1000)

    def format_string_node(self, client, key, data):

        node = {
            "type": "string",
            "redis_type": "string",
            "schemaName": client.db_name,
            "tableName": str(key, encoding='utf-8'),
            "name": client.db_name,
            "path": os.path.join(client.db_name, str(key, encoding='utf-8')),
            "pkColumnName": "",
            "columns": [
                {
                    "name": "Value",
                    "type": 5,
                    "typeName": "text",
                    "isAutoIncrement": "NO",
                    "isNullable": "NO",
                    "classification": "STRING",
                    "remark": "",
                    "isPath": "NO"
                }
            ],
            "rows": [data],
            "count": 1
        }
        self.logger.info(node)
        return node

    def get_files(self, node):
        self.logger.info('get get_files:')
        self.logger.info(node)
        rows = node['rows']
        rows = QueryDict(rows=rows)
        node['size'] = len(rows.value)
        md5 = hashlib.md5()
        md5.update(rows.value)
        node['md5'] = md5.hexdigest()
        return rows

    def get_nodes(self, path=''):
        self.logger.info('path:')
        self.logger.info(path)
        client = self.init_client(node=path)
        for k, t in self.keys_iter(client):
            # node['type'] = t.decode()
            # node['path'] = path['name']+'/'+k.decode()
            method = getattr(self, t.decode() + '_nodes', None)
            if method:
                yield from method(client, k)

    def string_nodes(self, client, k):
        self.logger.info('string node:')
        try:
            node = self.format_string_node(
                client, key=k, data=[client.get(k).decode('utf-8')])
            yield node
        except Exception:
            self.logger.error('decode failed:')
            self.logger.error(k)

    def hash_nodes(self, client, key):
        self.logger.info('hash_nodes:')

        def gen():
            for k, v in client.hscan_iter(name=key):
                if not v:
                    continue
                self.logger.debug(k)
                self.logger.debug(v)
                try:
                    yield k.decode('utf-8'), v.decode('utf-8')
                except UnicodeDecodeError:
                    self.logger.error(k)
                    self.logger.error(v)
                    self.logger.error(client)
                    self.logger.error(traceback.format_exc())

        for item in gen_send_data(gen(), limit=self.limit):
            self.logger.debug('hash item:')
            self.logger.debug(item)
            node = self.format_hash_node(client, key, data=item)
            yield node

    def format_hash_node(self, client, key, data):

        node = {
            "type": "string",
            "redis_type": "hash",
            "schemaName": client.db_name,
            "tableName": str(key, encoding='utf-8'),
            "name": client.db_name,
            "path": os.path.join(client.db_name, str(key, encoding='utf-8')),
            "columns": [
                {
                    "name": "key",
                    "type": 5,
                    "typeName": "text",
                    "isAutoIncrement": "NO",
                    "isNullable": "NO",
                    "classification": "STRING",
                    "remark": "",
                    "isPath": "NO"
                },
                {
                    "name": "value",
                    "type": 5,
                    "typeName": "text",
                    "isAutoIncrement": "NO",
                    "isNullable": "NO",
                    "classification": "STRING",
                    "remark": "",
                    "isPath": "NO"
                }
            ],
            "rows": data,
            "count": len(data)
        }
        self.logger.info(node)
        return node

    def list_nodes(self, client, key):
        def gen():
            for index in range(client.llen(key)):
                yield index + 1, client.lindex(key, index).decode()

        for item in gen_send_data(gen(), limit=self.limit):
            self.logger.debug('list_nodes:')
            self.logger.debug(item)
            node = self.format_list_node(client, key, item)
            yield node

    def format_list_node(self, client, key, data):

        data = [d for d in data]

        node = {
            "type": "string",
            "redis_type": "list",
            "schemaName": client.db_name,
            "tableName": str(key, encoding='utf-8'),
            "name": client.db_name,
            "path": os.path.join(client.db_name, str(key, encoding='utf-8')),
            "pkColumnName": "",
            "columns": [
                {
                    "name": "index",
                    "type": 5,
                    "typeName": "text",
                    "isAutoIncrement": "NO",
                    "isNullable": "NO",
                    "classification": "STRING",
                    "remark": "",
                    "isPath": "NO"
                },
                {
                    "name": "value",
                    "type": 5,
                    "typeName": "text",
                    "isAutoIncrement": "NO",
                    "isNullable": "NO",
                    "classification": "STRING",
                    "remark": "",
                    "isPath": "NO"
                }
            ],
            "rows": data,
            "count": len(data)
        }
        self.logger.info(node)
        return node

    def set_nodes(self, client, key):
        def gen():
            for k in client.sscan_iter(name=key):
                yield k.decode()

        for item in gen_send_data(gen(), limit=self.limit):
            self.logger.debug('set item:')
            self.logger.debug(item)
            node = self.format_set_node(client, key, item)
            yield node

    def format_set_node(self, client, key, data):

        node = {
            "type": "string",
            "redis_type": "set",
            "schemaName": client.db_name,
            "tableName": str(key, encoding='utf-8'),
            "name": client.db_name,
            "path": os.path.join(client.db_name, str(key, encoding='utf-8')),
            "pkColumnName": "",
            "columns": [
                {
                    "name": "member",
                    "type": 5,
                    "typeName": "text",
                    "isAutoIncrement": "NO",
                    "isNullable": "NO",
                    "classification": "STRING",
                    "remark": "",
                    "isPath": "NO"
                }
            ],
            "rows": data,
            "count": len(data)
        }
        self.logger.info(node)
        return node

    def zset_nodes(self, client, key):
        def gen():
            for value, score in client.zscan_iter(name=key):
                yield value.decode(), str(score)

        for item in gen_send_data(gen(), limit=self.limit):
            self.logger.debug('zset item:')
            self.logger.debug(item)
            node = self.format_zset_node(client, key, item)
            yield node

    def format_zset_node(self, client, key, data):

        node = {
            "type": "string",
            "redis_type": "zset",
            "schemaName": client.db_name,
            "tableName": str(key, encoding='utf-8'),
            "name": client.db_name,
            "path": os.path.join(client.db_name, str(key, encoding='utf-8')),
            "pkColumnName": "",
            "columns": [
                {
                    "name": "value",
                    "type": 5,
                    "typeName": "text",
                    "isAutoIncrement": "NO",
                    "isNullable": "NO",
                    "classification": "STRING",
                    "remark": "",
                    "isPath": "NO"
                },
                {
                    "name": "score",
                    "type": 5,
                    "typeName": "text",
                    "isAutoIncrement": "NO",
                    "isNullable": "NO",
                    "classification": "STRING",
                    "remark": "",
                    "isPath": "NO"
                }
            ],
            "rows": data,
            "count": len(data)

        }
        self.logger.info(node)
        return node


class RedisIterator(object):
    def __init__(self, auth, resources):
        self.auth = auth
        self.client = RedisBatch(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = dict()
        self.logger = logging.getLogger(self.__class__.__name__)

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = Dict(self.stack.pop())
            if 'children' in self.node:
                if self.node['children']:
                    children = self.node['children']
                else:
                    children = self.client.get_nodes(self.node)
                for child in children:
                    self.stack.append(child)
            else:
                # self.node['path'] = str(uuid.uuid4())
                # self.logger.info('self.node:')
                # self.logger.info(self.node)
                return self.node, partial(self.client.get_files, self.node)
        raise StopIteration


if __name__ == '__main__':
    auth = {
        'port': 6379,
        'ip': '192.190.10.120',
        'password': 'Spinfo0123'
    }
    r = RedisOne(**auth)
    print(r.get_nodes())
