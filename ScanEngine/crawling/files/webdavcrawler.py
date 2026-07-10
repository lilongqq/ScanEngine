#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import io
import hashlib
import os.path
import posixpath
from tempfile import SpooledTemporaryFile
from collections.abc import MutableMapping
from collections import UserDict
from functools import singledispatchmethod, partial
from crawling.interface import Iterator, Client
from common.conntool import request_limit, weak_cache
from common.functools import gen_key
from webdav4.client import Client as WebDavClient


class WebDavDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['last_write_time']
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


class WebDav(Client):

    def __init__(self, url, username, password):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = WebDavClient(
            url,
            auth=(
                username,
                password
            )
        )

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if node and node.get('path', node['name']):
            path = node.get('path', node['name'])
        else:
            path = ''
        resp = self.client.ls(path)
        for item in resp:
            if item['type'] == 'directory':
                node = self.format_dir(item)
            else:
                node = self.format_file(item)
            nodes.append(node)
        return nodes

    @get_nodes.register(str)
    def _(self, path):
        nodes = list()
        resp = self.client.ls(path)
        for item in resp:
            if item['type'] == 'directory':
                node = self.format_dir(item)
            else:
                node = self.format_file(item)
            nodes.append(node)
        return nodes

    @singledispatchmethod
    def get_file(self, *args, **kwargs):
        raise NotImplemented

    @get_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        self.client.download_fileobj(path, f)
        node['size'] = f.tell()
        f.seek(0)
        return f

    @get_file.register(str)
    def _(self, path):
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        self.client.download_fileobj(path, f)
        f.seek(0)
        return f

    @singledispatchmethod
    def delete_file(self, *args, **kwargs):
        raise NotImplemented

    @delete_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        self.client.remove(path)

    @delete_file.register(str)
    def _(self, path):
        self.client.remove(path)

    @singledispatchmethod
    def put_file(self, *args, **kwargs):
        raise NotImplemented

    @put_file.register(MutableMapping)
    def _(self, node, f):
        path = node.get('path', node['name'])
        self.client.upload_fileobj(file_obj=f, to_path=path, overwrite=True)

    @put_file.register(str)
    def _(self, path, f):
        self.client.upload_fileobj(file_obj=f, to_path=path, overwrite=True)

    def empty_file(self, node):
        f = io.BytesIO()
        self.put_file(node, f)

    def store_file(self, node, f):
        path = posixpath.join(node.get('sepPath', ''), node['name'])
        self.put_file(path, f)

    @staticmethod
    def format_dir(item):
        node = dict()
        node['type'] = 'dir'
        node['name'] = item['display_name']
        node['path'] = item['name']
        node['last_write_time'] = item['modified'].timestamp()
        node['children'] = []
        return node

    @staticmethod
    def format_file(item):
        node = dict()
        node['type'] = 'file'
        node['name'] = item['display_name']
        node['path'] = item['name']
        node['size'] = item['content_length']
        node['last_write_time'] = item['modified'].timestamp()
        node['etag'] = item['etag']
        return node

    def __bool__(self):
        return True

    def __del__(self):
        self.client.http.close()


@weak_cache
class WebDavOne(WebDav):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@weak_cache
class NutCloud(WebDav):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class WebDavBatch(WebDav):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@request_limit(period=1800, counts=500)
class NutCloudBatch(WebDav):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class WebDavIterator(Iterator):

    def __init__(self, auth, resources):
        self.auth = auth
        self.client = WebDavBatch(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = WebDavDict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = WebDavDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'WEBDAV'
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


class NutIterator(Iterator):

    def __init__(self, auth, resources):
        self.auth = auth
        self.client = NutCloudBatch(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = WebDavDict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = WebDavDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'Nut'
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
