#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import io
import hashlib
import os.path
import ssl
from functools import partial
from datetime import datetime
from imaplib import IMAP4, IMAP4_SSL
from collections import UserDict
from crawling.interface import Iterator, Client
from common.globals import Variables
from common.conntool import client_pool, weak_cache
from common.functools import boolean


class IMAPDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return datetime.strptime(self.data['Date'], '%a, %d %b %Y %H:%M:%S %z').timestamp()
        elif item == 'diff':
            return self.data.get('md5')
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'Date' in self.data
        elif item == 'diff':
            return 'md5' in self.data
        else:
            return item in self.data


@weak_cache
@client_pool(max_connections=1)
class IMAP(Client):

    def __init__(self,
                 username,
                 password,
                 host,
                 port,
                 keyfile=None,
                 certfile=None,
                 starttls=False,
                 timeout=None,
                 ssl_wrap=False
                 ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.keyfile = keyfile
        self.certfile = certfile
        self.starttls = boolean(starttls)
        self.timeout = timeout
        self.ssl_wrap = boolean(ssl_wrap)
        if self.ssl_wrap:
            self.client = IMAP4_SSL(host=self.host,
                                    port=self.port,
                                    keyfile=self.keyfile,
                                    certfile=self.certfile)
        else:
            self.client = IMAP4(host=self.host,
                                port=self.port)
            if self.starttls:
                context = getattr(ssl, '_create_unverified_context')(
                    keyfile=self.keyfile,
                    certfile=self.certfile
                )
                self.client.starttls(context)
        self.client.login(user=self.username, password=self.password)
        self.email = self.variables.email
        self.ids = self.email['id']
        if self.username.split('@')[1] in self.ids:
            getattr(self.client, '_simple_command')(
                'ID',
                '("name" "zhangl" "version" "1.0.0" "vendor" "shipinginfo.com")'
            )

    def get_nodes(self, node={}):
        self.logger.info(node)
        nodes = list()
        if node and (
                box_name := node.get('box_name')
        ):
            self.client.select(box_name, readonly=True)
            typ, data = self.client.search(None, 'ALL')
            if typ == 'OK':
                for num in data[0].decode().split():
                    node = dict()
                    node['num'] = num
                    node['primary_smtp_address'] = self.username
                    node['box_name'] = box_name
                    node['type'] = 'file'
                    node['name'] = num + '.eml'
                    node['path'] = os.path.join(self.username, box_name, node['name'])
                    nodes.append(node)
        else:
            typ, boxes = self.client.list()
            if typ == 'OK':
                for box in boxes:
                    name, box_name = self.format_box(box)
                    if box_name not in ('/', 'Deleted'):
                        node = dict()
                        node['primary_smtp_address'] = self.username
                        node['box_name'] = box_name
                        node['type'] = 'file'
                        node['name'] = name
                        node['path'] = os.path.join(self.username, box_name)
                        node['children'] = []
                        nodes.append(node)
        return nodes

    @staticmethod
    def format_box(box):
        box_name = box.decode().split()[2].strip('""')
        box_tuple = box_name.split('/')
        if len(box_tuple) == 2:
            name = box_tuple[1]
        else:
            name = box_tuple[0]
        return name, box_name

    def get_file(self, node):
        self.client.select(node['box_name'], readonly=True)
        typ, datas = self.client.fetch(node['num'], '(RFC822)')
        if typ == 'OK':
            data = datas[0][1]
            node['size'] = len(data)
            f = io.BytesIO(data)
            return f
        else:
            raise OSError()

    def __bool__(self):
        try:
            self.client.noop()
        except OSError:
            return False
        else:
            return True

    def __del__(self):
        if self:
            self.client.logout()


class MultiIMAP(object):

    def __init__(self,
                 host,
                 port,
                 keyfile=None,
                 certfile=None,
                 starttls=False,
                 timeout=None,
                 ssl_wrap=False,
                 accounts={}):
        self.host = host
        self.port = port
        self.keyfile = keyfile
        self.certfile = certfile
        self.starttls = starttls
        self.timeout = timeout
        self.ssl_wrap = ssl_wrap
        self.accounts = accounts

    @staticmethod
    def __primary_smtp_address(path):
        path_tuple = path.split('/', 1)
        if len(path_tuple) == 2:
            primary_smtp_address = path_tuple[0]
        else:
            primary_smtp_address = path
        return primary_smtp_address

    def get_nodes(self, node={}):
        kwargs = dict()
        if node and (
                path := node.get('path', node['name'])
        ):
            primary_smtp_address = self.__primary_smtp_address(path)
            kwargs['username'] = primary_smtp_address
            kwargs['password'] = self.accounts.get(primary_smtp_address)
            kwargs['host'] = self.host
            kwargs['port'] = self.port
            kwargs['keyfile'] = self.keyfile
            kwargs['certfile'] = self.certfile
            kwargs['starttls'] = self.starttls
            kwargs['timeout'] = self.timeout
            kwargs['ssl_wrap'] = self.ssl_wrap
            client = IMAP(**kwargs)
            return client.get_nodes(node)

    def get_file(self, node={}):
        kwargs = dict()
        if node and (
                path := node.get('path', node['name'])
        ):
            primary_smtp_address = self.__primary_smtp_address(path)
            kwargs['username'] = primary_smtp_address
            kwargs['password'] = self.accounts.get(primary_smtp_address)
            kwargs['host'] = self.host
            kwargs['port'] = self.port
            kwargs['keyfile'] = self.keyfile
            kwargs['certfile'] = self.certfile
            kwargs['starttls'] = self.starttls
            kwargs['timeout'] = self.timeout
            kwargs['ssl_wrap'] = self.ssl_wrap
            client = IMAP(**kwargs)
            return client.get_file(node)


class MultiIMAPIterator(Iterator):

    def __init__(self, auth, resources):
        self.auth = auth
        self.client = MultiIMAP(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = IMAPDict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = IMAPDict(self.stack.pop())
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
        md5 = hashlib.md5()
        md5.update(f.getbuffer())
        node['md5'] = md5.hexdigest()
        return f.getvalue()
