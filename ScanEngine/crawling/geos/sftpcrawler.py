#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import logging
import stat
import hashlib
import os.path
from . import must_combined_suffixes, combined_suffixes_map
from tempfile import SpooledTemporaryFile
from collections.abc import MutableMapping
from collections import UserDict, ChainMap
from functools import singledispatchmethod, partial
from paramiko import Transport, SFTPClient, AuthenticationException
from paramiko.rsakey import RSAKey
from paramiko.dsskey import DSSKey
from paramiko.ecdsakey import ECDSAKey
from paramiko.ed25519key import Ed25519Key
from crawling.interface import Iterator, Client
from common.functools import gen_key
from common.conntool import client_pool, weak_cache, thread_local


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
        if pkey:
            if self.keyt == 'RSA':
                self.pkey = RSAKey.from_private_key(
                    io.StringIO(pkey),
                    password=self.password
                )
            elif self.keyt == 'DSS':
                self.pkey = DSSKey.from_private_key(
                    io.StringIO(pkey),
                    password=self.password
                )
            elif self.keyt == 'ECDSA':
                self.pkey = ECDSAKey.from_private_key(
                    io.StringIO(pkey),
                    password=self.password
                )
            elif self.keyt == 'Ed25519':
                self.pkey = Ed25519Key.from_private_key(
                    io.StringIO(pkey),
                    password=self.password
                )
            else:
                raise ValueError(self.keyt)
            try:
                self.transport = Transport(
                    self.sock,
                    disabled_algorithms={
                        'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']
                    }
                )
                self.transport.connect(
                    username=self.username,
                    pkey=self.pkey
                )
            except AuthenticationException:
                self.transport = Transport(self.sock)
                self.transport.connect(
                    username=self.username,
                    pkey=self.pkey
                )
        else:
            self.transport = Transport(self.sock)
            self.transport.connect(
                username=self.username,
                password=self.password
            )
        self.transport.set_keepalive(30)
        self.transport.sock.settimeout(600)
        self.client = SFTPClient.from_transport(self.transport)

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
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        self.logger.info('getfo')
        self.logger.info('path: {}'.format(path))
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

    def delete_file(self, node):
        path = node['path']
        self.client.remove(path)

    def __bool__(self):
        return self.transport.is_active()

    def __del__(self):
        self.transport.close()


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
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.client = SFTPBatch(**self.auth)
        self.stack = list()
        if resources:
            self.stack.append(resources)
        self.dir = dict()
        self.node = SFTPDict()
        self.adf_files = list()
        self.isolated_files = list()
        self.subfiles = list()
        self.subdirs = list()
        self.combined_files = dict()
        self.value_suffixes_map = ChainMap(*combined_suffixes_map.values())

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.isolated_files:
                self.node = SFTPDict(self.isolated_files.pop())
                self.logger.debug(self.node)
                return self.node, partial(self.get_file, self.node)
            elif self.subfiles:
                for node in self.subfiles:
                    if node['name'].lower() == 'tileset.json':
                        for key, value in self.combined_files.items():
                            if value['suffixes'][0] == '.b3dm':
                                value['size'] += node['size']
                                value['suffixes'].append(node['suffix'])
                                value['children'].append(node)
                                self.logger.info(
                                    'b3dm tileset.json combined_files: {}'.format(
                                        key
                                    )
                                )
                                break
                    else:
                        for key, value in self.combined_files.items():
                            if key in node['name'].lower():
                                if node['name'].lower().removeprefix(key).isascii():
                                    value['size'] += node['size']
                                    value['suffixes'].append(node['suffix'])
                                    value['children'].append(node)
                                    break
                self.subfiles.clear()
            elif self.combined_files:
                key, value = self.combined_files.popitem()
                self.logger.info('combined_files: {}'.format(key))
                if set(value['suffixes']).issuperset(must_combined_suffixes[value['suffixes'][0]]):
                    self.node = SFTPDict(value)
                    self.logger.debug(self.node)
                    return self.node, partial(self.get_entirety, self.node)
                else:
                    self.logger.info(
                        'check integrity failed ignore combined_files: {}'.format(
                            value
                        )
                    )
            elif self.subdirs:
                if self.adf_files:
                    self.handle_adf_files()
                else:
                    for node in self.subdirs:
                        if node['name'].lower() == 'info':
                            break
                    else:
                        self.stack.extend(self.subdirs)
                        self.subdirs.clear()
                        continue
                    self.handle_adf_files()
            elif self.adf_files:
                self.node = SFTPDict(self.dir)
                self.node['type'] = 'entirety'
                size = 0
                for node in self.adf_files:
                    self.node['children'].append(node)
                    size += node['size']
                else:
                    self.node['size'] = size
                    self.node['name'] = 'coverage.adf'
                    self.adf_files.clear()
                self.logger.debug(self.node)
                return self.node, partial(self.get_entirety, self.node)
            elif self.stack:
                self.dir = self.stack.pop()
                if self.dir['children']:
                    children = self.dir['children']
                else:
                    children = self.get_nodes(self.dir)
                self.dir['children'] = []
                if self.dir['name'].lower().endswith('.gdb'):
                    self.node = SFTPDict(self.dir)
                    self.node['type'] = 'entirety'
                    self.node['children'] = children
                    return self.node, partial(self.get_entirety, self.node)
                else:
                    for node in children:
                        if node.get('type', 'dir') == 'dir':
                            self.subdirs.append(node)
                        else:
                            if self.handle_combined_key_file(node):
                                continue
                            if self.handle_combined_file(node):
                                continue
                            self.isolated_files.append(node)
            else:
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

    def get_entirety(self, node):
        files = list()
        md5 = hashlib.md5()
        for item in node['children']:
            f = self.get_file(item)
            files.append(f)
            md5.update(bytes.fromhex(item['md5']))
        node['md5'] = md5.hexdigest()
        return files

    def handle_combined_key_file(self, node):
        for suffix in combined_suffixes_map.keys():
            if node['name'].lower().endswith(suffix):
                node['suffix'] = suffix
                combined_dir = dict(self.dir)
                combined_dir['type'] = 'entirety'
                combined_dir['name'] = node['name']
                combined_dir['suffixes'] = [suffix]
                combined_dir['children'] = [node]
                combined_dir['size'] = node['size']
                self.combined_files[
                    node['name'].lower().removesuffix(suffix)
                ] = combined_dir
                return True
        else:
            return False

    def handle_combined_file(self, node):
        for suffix in self.value_suffixes_map.keys():
            if node['name'].lower().endswith(suffix):
                node['suffix'] = suffix
                self.subfiles.append(node)
                return True
        else:
            return False

    def handle_adf_files(self):
        while self.subdirs:
            node = self.subdirs.pop()
            if node['children']:
                children = node['children']
            else:
                children = self.get_nodes(node)
            node['children'] = []

            for item in children:
                if item['name'].lower().endswith(('.adf', '.dat')):
                    self.adf_files.extend(children)
                    break
            else:
                node['children'] = children
                self.stack.append(node)
