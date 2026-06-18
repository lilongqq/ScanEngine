#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import re
import io
import os.path
import hashlib
import zipfile
from datetime import datetime
from urllib.parse import urlparse, unquote_plus
from typing import Optional
from collections.abc import MutableMapping
from collections import UserDict
from functools import singledispatchmethod, partial
import requests
from crawling.interface import Iterator, Client
from common.functools import gen_key, boolean


class HttpGetDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['last_write_time']
        elif item == 'diff':
            if 'ETag' in self.data:
                return gen_key(
                    http_url=self.data.get('http_url'),
                    ETag=self.data.get('ETag')
                )
            else:
                return gen_key(
                    http_url=self.data.get('http_url'),
                    last_write_time=self.data.get('last_write_time')
                )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return {'last_write_time', 'http_url'}.issubset(self.data) or {'ETag', 'http_url'}.issubset(self.data)
        else:
            return item in self.data


class HttpGet(Client):

    def __init__(self, decompress=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.decompress = boolean(decompress)
        self.session = requests.session()
        self.session.verify = False
        self.pattern = re.compile(pattern=r'(?<=filename=).+?(?=(;|$))', flags=re.IGNORECASE)

    def get_nodes(self, url: str):
        nodes = list()
        node = dict()
        node['type'] = 'file'
        node['http_url'] = url
        resp = self.session.head(url)
        headers = resp.headers
        if 'Content-Disposition' in headers:
            match = self.pattern.search(headers['Content-Disposition'])
            if match:
                node['name'] = match.group()
            else:
                node['name'] = unquote_plus(os.path.basename(urlparse(url).path))
        if 'Content-Length' in headers:
            node['size'] = int(headers['Content-Length'])
        if 'Last-Modified' in headers:
            node['last_write_time'] = datetime.strptime(
                headers['Last-Modified'],
                '%a, %d %b %Y %H:%M:%S %Z').timestamp()
        if 'ETag' in headers:
            node['ETag'] = headers['ETag']
        node['path'] = url
        nodes.append(node)
        return nodes

    # @get_nodes.register(str)
    # def _(self, url: str):
    #     nodes = list()
    #     node = dict()
    #     node['type'] = 'file'
    #     node['http_url'] = url
    #     node['name'] = unquote_plus(os.path.basename(urlparse(url).path))
    #     node['path'] = url
    #     nodes.append(node)

    @singledispatchmethod
    def get_file(self, *args, **kwargs):
        raise NotImplemented

    @get_file.register(MutableMapping)
    def _(self, node):
        f = io.BytesIO()
        resp = self.session.get(node['http_url'], stream=True)
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
        if self.decompress:
            f.seek(0)
            if zipfile.is_zipfile(f):
                zf = zipfile.ZipFile(f)
                for info in zf.infolist():
                    if not info.is_dir():
                        data = zf.read(info)
                        node['size'] = len(data)
                        return io.BytesIO(data)
        else:
            node['size'] = f.tell()
        f.seek(0)
        return f

    @get_file.register(str)
    def _(self, url):
        f = io.BytesIO()
        resp = self.session.get(url, stream=True)
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
        if self.decompress:
            f.seek(0)
            if zipfile.is_zipfile(f):
                zf = zipfile.ZipFile(f)
                for info in zf.infolist():
                    if not info.is_dir():
                        data = zf.read(info)
                        return io.BytesIO(data)
        f.seek(0)
        return f

    def __del__(self):
        self.session.close()

    def __bool__(self):
        return True


class HttpGetBatch(HttpGet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class HttpGetIterator(Iterator):

    def __init__(self, auth, resources: Optional[MutableMapping] = None):
        self.auth = auth
        self.client = HttpGetBatch(**self.auth)
        self.stack = list()
        if resources:
            self.stack.append(resources)
        self.node = HttpGetDict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = HttpGetDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'HTTPGET'
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

    def __bool__(self):
        return bool(self.stack)

    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    def add_nodes(self, nodes):
        self.stack.extend(nodes)

    def get_file(self, node):
        f = self.client.get_file(node)
        md5 = hashlib.md5()
        md5.update(f.getbuffer())
        node['md5'] = md5.hexdigest()
        return f.getvalue()
