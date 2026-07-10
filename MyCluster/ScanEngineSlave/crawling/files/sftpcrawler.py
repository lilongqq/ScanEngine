#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import logging
import stat
import hashlib
import os.path
from collections.abc import MutableMapping
from collections import UserDict
from functools import singledispatchmethod, partial
from paramiko import SFTPClient, AuthenticationException
from paramiko.rsakey import RSAKey
from paramiko.dsskey import DSSKey
from paramiko.ecdsakey import ECDSAKey
from paramiko.ed25519key import Ed25519Key
from ex3rd.paramiko import Transport
from crawling.interface import Iterator, Client
from common.functools import gen_key
from common.conntool import thread_local, client_pool, weak_cache


class SFTPDict(UserDict):

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


class SFTP(Client):

    def __init__(self,
                 host,
                 port,
                 username,
                 password=None,
                 pkey=None,
                 keyt='RSA',
                 encoding='utf-8',
                 **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.host = host
        self.port = int(port)
        self.sock = (self.host, self.port)
        self.username = username
        self.password = password
        self.keyt = keyt
        self.encoding = encoding
        self.transport = self.create_transport(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            pkey=pkey,
            keyt=self.keyt
        )
        self.client = SFTPClient.from_transport(self.transport)

    @staticmethod
    @weak_cache
    def create_transport(
        host,
        port,
        username,
        password=None,
        pkey=None,
        keyt='RSA'
    ):
        sock = (host, port)
        if pkey:
            if keyt == 'RSA':
                pkey = RSAKey.from_private_key(
                    io.StringIO(pkey),
                    password=password
                )
            elif keyt == 'DSS':
                pkey = DSSKey.from_private_key(
                    io.StringIO(pkey),
                    password=password
                )
            elif keyt == 'ECDSA':
                pkey = ECDSAKey.from_private_key(
                    io.StringIO(pkey),
                    password=password
                )
            elif keyt == 'Ed25519':
                pkey = Ed25519Key.from_private_key(
                    io.StringIO(pkey),
                    password=password
                )
            else:
                raise ValueError(keyt)
            try:
                transport = Transport(
                    sock,
                    disabled_algorithms={
                        'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']
                    }
                )
                transport.connect(
                    username=username,
                    pkey=pkey
                )
            except AuthenticationException:
                transport.close()
                transport = Transport(sock)
                transport.connect(
                    username=username,
                    pkey=pkey
                )
        else:
            transport = Transport(sock)
            transport.connect(
                username=username,
                password=password
            )
        transport.set_keepalive(30)
        transport.sock.settimeout(600)
        return transport

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if node and node.get('path', node['name']):
            path = node.get('path', node['name'])
        else:
            path = "."
        self.logger.info('listdir_attr')
        self.logger.info('path: {}'.format(path))
        resp = self.client.listdir_attr(path)
        for item in resp:
            if stat.S_ISDIR(item.st_mode):
                node = self.format_dir(item)
                node['path'] = os.path.join(path, item.filename)
                nodes.append(node)
            elif stat.S_ISREG(item.st_mode):
                node = self.format_file(item)
                node['path'] = os.path.join(path, item.filename)
                nodes.append(node)
            else:
                continue
        return nodes

    @get_nodes.register(str)
    def _(self, path):
        nodes = list()
        if path.endswith('/'):
            self.logger.info('listdir_attr')
            self.logger.info('path: {}'.format(path))
            resp = self.client.listdir_attr(path)
            for item in resp:
                if stat.S_ISDIR(item.st_mode):
                    node = self.format_dir(item)
                    node['path'] = os.path.join(path, item.filename)
                    nodes.append(node)
                elif stat.S_ISREG(item.st_mode):
                    node = self.format_file(item)
                    node['path'] = os.path.join(path, item.filename)
                    nodes.append(node)
                else:
                    continue
        else:
            item = self.client.stat(path)
            item.filename = os.path.basename(path)
            if stat.S_ISDIR(item.st_mode):
                node = self.format_dir(item)
                node['path'] = path
                nodes.append(node)
            elif stat.S_ISREG(item.st_mode):
                node = self.format_file(item)
                node['path'] = path
                nodes.append(node)
        return nodes

    def get_file(self, node):
        path = node.get('path', node['name'])
        f = io.BytesIO()
        self.client.getfo(path, fl=f)
        f.seek(0)
        return f

    @staticmethod
    def format_dir(item):
        node = dict()
        node['type'] = 'dir'
        node['name'] = item.filename
        node['size'] = int(item.st_size)
        node['last_access_time'] = item.st_atime
        node['last_write_time'] = item.st_mtime
        node['children'] = []
        return node

    @staticmethod
    def format_file(item):
        node = dict()
        node['type'] = 'file'
        node['name'] = item.filename
        node['size'] = int(item.st_size)
        node['last_access_time'] = item.st_atime
        node['last_write_time'] = item.st_mtime
        return node

    @singledispatchmethod
    def put_file(self, *args, **kwargs):
        raise NotImplemented

    @put_file.register(MutableMapping)
    def _(self, node, f):
        path = node.get('path', None)
        self.client.putfo(f, remotepath=path)

    @put_file.register(MutableMapping)
    def _(self, path, f):
        self.client.putfo(f, remotepath=path)

    def delete_file(self, node):
        path = node.get('path', None)
        self.client.remove(path)

    def __bool__(self):
        return bool(self.client.get_channel().active)

    def __del__(self):
        self.client.close()


@thread_local
class SFTPBatch(SFTP):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@weak_cache
@client_pool(max_connections=3)
class SFTPOne(SFTP):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class SFTPIterator(Iterator):

    def __init__(self, auth, resources={}):
        self.auth = auth
        self.client = SFTPBatch(**self.auth)
        self.stack = list()
        if resources:
            self.stack.append(resources)
        self.node = SFTPDict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = SFTPDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'SFTP'
            if 'children' in self.node:
                if self.node['children']:
                    children = self.node['children']
                else:
                    children = self.get_nodes(self.node)
                children.reverse()
                self.stack.extend(children)
            else:
                return self.node
        raise StopIteration

    def __bool__(self):
        return bool(self.stack)

    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    def add_nodes(self, nodes):
        self.stack.extend(nodes)

    @staticmethod
    def wrapper_node(node):
        return SFTPDict(node)

    def get_file(self, node):
        f = self.client.get_file(node)
        md5 = hashlib.md5()
        md5.update(f.getbuffer())
        node['md5'] = md5.hexdigest()
        return f.getvalue()
