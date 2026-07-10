# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import re
import logging
import os.path
import socket
import hashlib
from selectors import DefaultSelector, EVENT_READ
from collections.abc import MutableMapping
from collections import UserDict
from functools import singledispatchmethod, partial
from smb.SMBConnection import SMBConnection
from nmb.NetBIOS import NetBIOS
from smb.base import NotConnectedError
from crawling.interface import Iterator, Client
from common.functools import gen_key, boolean
from common.conntool import thread_local, client_pool, weak_cache


class SMBDict(UserDict):

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


class SMB(Client):

    def __init__(self,
                 username,
                 password,
                 host,
                 port=445,
                 remote_name='',
                 domain='',
                 use_ntlm_v2=True,
                 is_direct_tcp=True,
                 **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.username = username
        self.password = password
        self.ip = host
        self.port = int(port)
        self.my_name = socket.gethostname()
        if remote_name:
            self.remote_name = remote_name
        else:
            netbios = NetBIOS()
            names = netbios.queryIPForName(self.ip)
            if names:
                self.remote_name = names[0]
            else:
                self.remote_name = 'SMBServer'
        self.domain = domain
        self.use_ntlm_v2 = boolean(use_ntlm_v2)
        self.is_direct_tcp = boolean(is_direct_tcp)
        self.client = SMBConnection(
            username=self.username,
            password=self.password,
            my_name=self.my_name,
            remote_name=self.remote_name,
            domain=self.domain,
            use_ntlm_v2=self.use_ntlm_v2,
            is_direct_tcp=self.is_direct_tcp
        )
        connected = self.client.connect(ip=self.ip, port=self.port)
        if connected:
            self.logger.info('connected is {}'.format(connected))
        else:
            raise NotConnectedError()
        self.selector = DefaultSelector()
        self.selector.register(self.client.sock, EVENT_READ)

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if node and (
                shared_path := node.get('path', node['name'])
        ):
            service_name, path = self.__service_name_and_path(shared_path)
            self.logger.info('listPath')
            self.logger.info(
                'service_name: {}, path: {}'.format(service_name, path))
            shared_files = self.client.listPath(
                service_name=service_name,
                path=path
            )
            for shared_file in shared_files:
                if shared_file.isDirectory:
                    if shared_file.filename in ['.', '..']:
                        continue
                    node = self.format_dir(shared_file)
                else:
                    node = self.format_file(shared_file)
                node['path'] = os.path.join(shared_path, shared_file.filename)
                nodes.append(node)
        else:
            self.logger.info('listShares')
            shared_devices = self.client.listShares()
            for shared_device in shared_devices:
                if shared_device.isSpecial and not re.match(r'^[A-Z]\$$', shared_device.name):
                    continue
                node = self.format_dev(shared_device)
                node['path'] = shared_device.name
                nodes.append(node)
        return nodes

    @get_nodes.register(str)
    def _(self, shared_path):
        nodes = list()
        shared_path = shared_path.lstrip('/')
        if shared_path:
            service_name, path = self.__service_name_and_path(shared_path)
            if path == '/':
                self.logger.info('listPath')
                self.logger.info(
                    'service_name: {}, path: {}'.format(service_name, path))
                shared_files = self.client.listPath(
                    service_name=service_name, path=path)
                for shared_file in shared_files:
                    if shared_file.isDirectory:
                        if shared_file.filename in ['.', '..']:
                            continue
                        node = self.format_dir(shared_file)
                    else:
                        node = self.format_file(shared_file)
                    node['path'] = os.path.join(
                        shared_path, shared_file.filename)
                    nodes.append(node)
            else:
                self.logger.info('getAttributes')
                self.logger.info(
                    'service_name: {}, path: {}'.format(
                        service_name,
                        path
                    )
                )
                shared_file = self.client.getAttributes(
                    service_name=service_name,
                    path=path
                )
                if shared_file.isDirectory:
                    node = self.format_dir(shared_file)
                else:
                    node = self.format_file(shared_file)
                node['path'] = shared_path
                nodes.append(node)
        else:
            self.logger.info('listShares')
            shared_devices = self.client.listShares()
            for shared_device in shared_devices:
                if shared_device.isSpecial and not re.match(r'^[A-Z]\$$', shared_device.name):
                    continue
                node = self.format_dev(shared_device)
                node['path'] = shared_device.name
                nodes.append(node)
        return nodes

    def get_file(self, node):
        self.logger.info('node:')
        self.logger.info(node)
        if node and (
                shared_path := node.get('path', node['name'])
        ):
            service_name, path = self.__service_name_and_path(shared_path)
            self.logger.info('retrieveFile')
            self.logger.info(
                'service_name: {}, path: {}'.format(service_name, path))
            f = io.BytesIO()
            self.client.retrieveFile(
                service_name=service_name,
                path=path,
                file_obj=f
            )
            f.seek(0)
            return f
        else:
            raise ValueError('shared_path is None')

    def store_file(self, node, f):
        if node:
            sep_path = node.get('sepPath', None)
            if not sep_path:
                raise ValueError('sepPath is empty')
            file_name = node.get('name', None)
            if not file_name:
                raise ValueError('file name is empty')
            pathname = sep_path + '/' + file_name
            service_name, path = self.__service_name_and_path(pathname)
            try:
                self.client.createDirectory(service_name, self.__service_name_and_path(sep_path)[1])
            except Exception:
                pass
            self.client.storeFile(service_name, path, f, timeout=10)
        else:
            raise ValueError('node is None')

    def empty_file(self, node):
        self.logger.info('node:')
        self.logger.info(node)
        if node and (
                shared_path := node.get('path', node['name'])
        ):
            service_name, path = self.__service_name_and_path(shared_path)
            self.logger.info('retrieveFile')
            self.logger.info(
                'service_name: {}, path: {}'.format(service_name, path))
            f = io.BytesIO()
            self.client.storeFile(service_name, path, f, timeout=10)
        else:
            raise ValueError('shared_path is None')

    def delete_file(self, node):
        self.logger.info('delete_file:')
        self.logger.info(node)
        if node and (
                shared_path := node.get('path', node['name'])
        ):
            service_name, path = self.__service_name_and_path(shared_path)
            self.logger.info('service_name:')
            self.logger.info(service_name)
            self.logger.info('path:')
            self.logger.info(path)
            self.client.deleteFiles(
                service_name=service_name, path_file_pattern=path)
        else:
            raise ValueError('shared_path is None')

    def __bool__(self):
        if getattr(self, 'selector', False):
            events = self.selector.select(timeout=0)
            return not events
        else:
            return False

    def __del__(self):
        if getattr(self, 'client', False):
            self.client.close()
        if getattr(self, 'selector', False):
            self.selector.close()

    @staticmethod
    def format_dev(shared_device):
        node = dict()
        node['type'] = 'file'
        node['isSpecial'] = shared_device.isSpecial
        node['name'] = shared_device.name
        node['comments'] = shared_device.comments
        node['children'] = []
        return node

    @staticmethod
    def format_dir(shared_file):
        node = dict()
        node['type'] = 'file'
        node['name'] = shared_file.filename
        node['create_time'] = shared_file.create_time
        node['isReadOnly'] = shared_file.isReadOnly
        node['last_access_time'] = shared_file.last_access_time
        node['last_attr_change_time'] = shared_file.last_attr_change_time
        node['last_write_time'] = shared_file.last_write_time
        node['children'] = []
        return node

    @staticmethod
    def format_file(shared_file):
        node = dict()
        node['type'] = 'file'
        node['name'] = shared_file.filename
        node['size'] = shared_file.file_size
        node['create_time'] = shared_file.create_time
        node['isReadOnly'] = shared_file.isReadOnly
        node['last_access_time'] = shared_file.last_access_time
        node['last_attr_change_time'] = shared_file.last_attr_change_time
        node['last_write_time'] = shared_file.last_write_time
        return node

    @staticmethod
    def __service_name_and_path(path):
        path_tuple = path.split('/', 1)
        if len(path_tuple) == 2:
            service_name, path = path_tuple
        else:
            service_name = path
            path = '/'
        return service_name, path


@thread_local
class SMBBatch(SMB):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@weak_cache
@client_pool(max_connections=3)
class SMBOne(SMB):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class SMBIterator(Iterator):

    def __init__(self, auth, resources={}):
        self.auth = auth
        self.client = SMBBatch(**self.auth)
        self.stack = list()
        if resources:
            self.stack.append(resources)
        self.node = SMBDict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = SMBDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'SMB'
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
        return SMBDict(node)

    def get_file(self, node):
        f = self.client.get_file(node)
        md5 = hashlib.md5()
        md5.update(f.getbuffer())
        node['md5'] = md5.hexdigest()
        return f.getvalue()
