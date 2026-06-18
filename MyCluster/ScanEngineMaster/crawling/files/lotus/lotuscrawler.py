#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64
import io
import logging
import uuid
import os.path
import hashlib
from collections import UserDict
from xml.etree import ElementTree
from .service.document import LotusDocumentService
from .service.mail import LotusMailService
from .struct.ttypes import LotusSwitchPkg, MailCondition
from thrift.protocol import TBinaryProtocol, TMultiplexedProtocol
from thrift.transport import TSocket, TTransport
from collections.abc import MutableMapping
from common.globals import Variables
from exceptions import LotusError
from crawling.interface import Iterator, Client
from functools import singledispatchmethod, partial
from common.conntool import alive_check, client_pool, weak_cache
from common.functools import gen_key


class LotusDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['last_write_time']
        elif item == 'diff':
            return gen_key(
                name=self.data.get('name'),
                notes_url=self.data.get('notes_url'),
                last_write_time=self.data.get('last_write_time')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return {'name', 'notes_url', 'last_write_time'}.issubset(self.data)
        else:
            return item in self.data


class Lotus(Client):

    def __init__(
            self,
            username,
            password,
            host,
            port=63148,
            filter_type='',
            start_time='',
            end_time='',
            service_name='mail',
            **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.service_name = service_name
        self.variables = Variables()
        self.socket = self.get_socket(**self.variables.lotus['socket'])
        self.client_id = str(uuid.uuid4())
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.transport.open()
        self.binary_protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.protocol = TMultiplexedProtocol.TMultiplexedProtocol(
            self.binary_protocol,
            self.service_name
        )
        if self.service_name == 'mail':
            self.client = LotusMailService.Client(self.protocol)
        elif self.service_name == 'document':
            self.client = LotusDocumentService.Client(self.protocol)
        else:
            raise ValueError()
        self.host = host
        self.port = int(port)
        self.username = username
        self.password = password
        self.filter_type = filter_type
        self.start_time = start_time
        self.end_time = end_time
        self.open()

    @staticmethod
    def get_socket(ip, port, timeout):
        socket = TSocket.TSocket(ip, port)
        socket.setTimeout(timeout)
        return socket

    def open(self):
        result = self.client.open(
            LotusSwitchPkg(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                client_id=self.client_id
            )
        )
        if result.code == 0:
            return True
        else:
            raise LotusError(result.code)

    def close(self):
        result = self.client.close(
            LotusSwitchPkg(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                client_id=self.client_id
            )
        )
        if result.code == 0:
            return True
        else:
            raise LotusError(result.code)

    def __bool__(self):
        return self.transport.isOpen()

    def __del__(self):
        if self.transport.isOpen():
            self.logger.info('client close')
            self.close()
            self.logger.info('transport close')
            self.transport.close()

    def get_paths(self):
        result = self.client.get_paths(
            MailCondition(
                start_time=self.start_time,
                end_time=self.end_time,
                filter_type=self.filter_type,
                client_id=self.client_id
            )
        )
        if result.code == 0:
            return result.paths
        else:
            raise LotusError(result.code)

    def get_counts(self, path):
        result = self.client.get_counts(
            MailCondition(
                path=path,
                start_time=self.start_time,
                end_time=self.end_time,
                filter_type=self.filter_type,
                client_id=self.client_id
            )
        )
        if result.code == 0:
            return result.count
        else:
            raise LotusError(result.code)

    def get_metadata(self, path, index):
        result = self.client.get_metadata(
            MailCondition(
                path=path,
                index=index,
                start_time=self.start_time,
                end_time=self.end_time,
                filter_type=self.filter_type,
                client_id=self.client_id
            )
        )
        if result.code == 0:
            return result
        else:
            raise LotusError(result.code)

    def get_xml(self, path, index):
        result = self.client.get_xml(
            MailCondition(
                path=path,
                index=index,
                start_time=self.start_time,
                end_time=self.end_time,
                filter_type=self.filter_type,
                client_id=self.client_id
            )
        )
        if result.code == 0:
            return result.xml
        else:
            raise LotusError(result.code)

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if node and (
                user := node.get('user')
        ):
            count = self.get_counts(user)
            for index in range(1, count + 1):
                node = dict()
                node['type'] = 'file'
                node['index'] = index
                node['name'] = '{}.xml'.format(index)
                node['path'] = os.path.join(user, node['name'])
                node['user'] = user
                nodes.append(node)
        else:
            users = self.get_paths()
            for user in users:
                node = dict()
                node['type'] = 'file'
                node['name'] = user
                node['path'] = user
                node['user'] = user
                node['children'] = []
                nodes.append(node)
        return nodes

    @get_nodes.register(str)
    def _(self, user):
        nodes = list()
        if user:
            count = self.get_counts(user)
            for index in range(1, count + 1):
                node = dict()
                node['type'] = 'file'
                node['index'] = index
                node['name'] = '{}.xml'.format(index)
                node['path'] = os.path.join(user, node['name'])
                node['user'] = user
                nodes.append(node)
        else:
            users = self.get_paths()
            for user in users:
                node = dict()
                node['type'] = 'file'
                node['name'] = user
                node['path'] = user
                node['user'] = user
                node['children'] = []
                nodes.append(node)
        return nodes

    def get_file(self, node):
        path = node['user']
        index = node['index']
        return self.get_xml(path=path, index=index)


@weak_cache
@client_pool(max_connections=1)
class LotusOne(Lotus):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@alive_check
class LotusBatch(Lotus):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class LotusIterator(Iterator):

    def __init__(self, auth, resources):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.auth = auth
        self.client = LotusBatch(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = dict()
        self.files = list()

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if len(self.files) > 0:
                node, data = self.files.pop()
                return node, partial(self.wrapper_file, node, data)
            elif len(self.stack) > 0:
                self.node = self.stack.pop()
                if 'children' in self.node:
                    if self.node['children']:
                        children = self.node['children']
                    else:
                        children = self.get_nodes(self.node)
                    children.reverse()
                    self.stack.extend(children)
                elif 'index' in self.node:
                    metadata = self.client.get_metadata(
                        self.node['user'],
                        self.node['index']
                    )
                    self.node['create_time'] = metadata.create_time / 1000
                    self.node['last_write_time'] = metadata.modify_time / 1000
                    self.node['last_access_time'] = metadata.access_time / 1000
                    self.node['size'] = metadata.size
                    self.node['notes_url'] = metadata.notes_url
                    self.node['http_url'] = metadata.http_url
                    xml = self.client.get_xml(
                        self.node['user'],
                        self.node['index']
                    )
                    root = ElementTree.fromstring(xml)
                    item = root.find(self.variables.lotus['email']['Subject'])
                    if item is not None:
                        if item.text:
                            self.node['Subject'] = item.text
                    item = root.find(self.variables.lotus['email']['From'])
                    if item is not None:
                        if item.text:
                            self.node['From'] = item.text
                    items = root.findall(self.variables.lotus['email']['To'])
                    if items:
                        texts = [item.text for item in items]
                        if texts and texts[0]:
                            self.node['To'] = texts
                    items = root.findall(self.variables.lotus['email']['cc'])
                    if items:
                        texts = [item.text for item in items]
                        if texts and texts[0]:
                            self.node['cc'] = texts
                    items = root.findall(self.variables.lotus['email']['bcc'])
                    if items:
                        texts = [item.text for item in items]
                        if texts and texts[0]:
                            self.node['bcc'] = texts
                    items = root.findall(self.variables.lotus['file']['item'])
                    for item in items:
                        file = item.find(self.variables.lotus['file']['file'])
                        if file is not None:
                            node = dict()
                            node.update(self.node)
                            node['name'] = file.get('name', '')
                            node['size'] = int(file.get('size', 0))
                            node['path'] = os.path.join(
                                node['path'],
                                node['name']
                            )
                            filedata = file.find(
                                self.variables.lotus['file']['filedata']
                            )
                            if filedata is not None:
                                data = base64.b64decode(filedata.text)
                                self.files.append((LotusDict(node), data))
                        root.remove(item)
                    body = io.BytesIO()
                    ElementTree.ElementTree(root).write(body, encoding='utf-8')
                    node = dict()
                    node.update(self.node)
                    node['name'] = 'body'
                    node['path'] = os.path.join(node['path'], node['name'])
                    self.files.append((LotusDict(node), body.getvalue()))
            else:
                raise StopIteration

    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    @staticmethod
    def wrapper_file(node, data):
        md5 = hashlib.md5()
        md5.update(data)
        node['md5'] = md5.hexdigest()
        node['size'] = len(data)
        return data
