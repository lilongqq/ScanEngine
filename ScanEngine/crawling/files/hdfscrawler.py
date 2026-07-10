#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import hashlib
import os.path
import logging
from tempfile import SpooledTemporaryFile
from functools import singledispatchmethod, partial
from collections.abc import MutableMapping
from collections import UserDict
from ex3rd.hdfs import SecureClient
from common.conntool import alive_check, weak_cache
from common.functools import gen_key
from crawling.interface import Iterator


class HDFSDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data.get('last_write_time')
        elif item == 'diff':
            return gen_key(
                path=self.data.get('path'),
                last_write_time=self.data.get('last_write_time')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return {'last_write_time', 'path'}.issubset(self.data)
        else:
            return item in self.data


class HDFS(object):

    def __init__(self, url, username='root', cert=None, verify_ssl=False, **kwargs):
        if isinstance(verify_ssl, str):
            verify_ssl = verify_ssl.lower() == 'true'
        self.client = SecureClient(url, user=username, cert=cert, verify=verify_ssl)

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if node and (node.get('path') or node.get('name')):
            path = node.get('path') or node.get('name')
        else:
            path = '/'
        resp = self.client.list(path, status=True)
        for name, status in resp:
            node = self.format(path, name, status)
            nodes.append(node)
        return nodes

    @get_nodes.register(str)
    def _(self, path):
        nodes = list()
        if not path:
            path = '/'
        if path.endswith('/'):
            resp = self.client.list(path, status=True)
            for name, status in resp:
                node = self.format(path, name, status)
                nodes.append(node)
        else:
            status = self.client.status(path)
            if status['type'] == 'DIRECTORY':
                resp = self.client.list(path, status=True)
                for name, status in resp:
                    node = self.format(path, name, status)
                    nodes.append(node)
            else:
                name = os.path.basename(path)
                dir_path = os.path.dirname(path)
                node = self.format(dir_path, name, status)
                nodes.append(node)
        return nodes

    def get_file(self, node={}):
        path = node.get('path') or node.get('name')
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        with self.client.read(path) as resp:
            for chunk in resp:
                f.write(chunk)
        f.seek(0)
        return f

    def __bool__(self):
        return True

    def __del__(self):
        return True

    @staticmethod
    def format(path, name, status):
        node = dict()
        if status['type'] == 'DIRECTORY':
            node['type'] = 'dir'
            node['name'] = name
            node['childrenNum'] = status['childrenNum']
            node['fileId'] = status['fileId']
            node['group'] = status['group']
            node['owner'] = status['owner']
            node['permission'] = status['permission']
            node['path'] = os.path.join(path, name)
            node['children'] = []
        if status['type'] == 'FILE':
            node['type'] = 'file'
            node['name'] = name
            node['size'] = status['length']
            node['fileId'] = status['fileId']
            node['group'] = status['group']
            node['owner'] = status['owner']
            node['permission'] = status['permission']
            node['accessTime'] = status['accessTime']
            node['last_write_time'] = status['modificationTime']
            node['blockSize'] = status['blockSize']
            node['path'] = os.path.join(path, name)
        return node


@alive_check
class HDFSBatch(HDFS):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@weak_cache
class HDFSOne(HDFS):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class HDFSIterator(Iterator):

    def __init__(self, auth, resources):
        self.auth = auth
        self.client = HDFSBatch(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = HDFSDict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = HDFSDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'HDFS'
            if 'children' in self.node:
                if self.node['children']:
                    children = self.node['children']
                else:
                    children = self.get_nodes(self.node)
                children.reverse()
                self.stack.extend(children)
            else:
                return self.node, partial(self.get_file, self.node)
        raise StopIteration

    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    def get_file(self, node):
        f = self.client.get_file(node)
        md5 = hashlib.file_digest(f, 'md5')
        node['md5'] = md5.hexdigest()
        f.seek(0)
        return f
