#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import io
import re
import hashlib
import os.path
import ssl
from functools import partial
from datetime import datetime
from poplib import POP3
from exstd.poplib import POP3_SSL
from collections import UserDict
from crawling.interface import Iterator, Client
from common.functools import boolean


class POP3Dict(UserDict):

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


class POP3One(Client):

    def __init__(self,
                 username,
                 password,
                 host,
                 port,
                 keyfile=None,
                 certfile=None,
                 starttls=False,
                 timeout=None,
                 ssl_wrap=False,
                 auth_type='simple'
                 ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.keyfile = keyfile
        self.certfile = certfile
        self.starttls = starttls
        self.timeout = timeout
        self.ssl_wrap = ssl_wrap
        if self.ssl_wrap:
            self.client = POP3_SSL(host=self.host,
                                   port=self.port,
                                   keyfile=self.keyfile,
                                   certfile=self.certfile,
                                   timeout=self.timeout)
        else:
            self.client = POP3(host=self.host,
                               port=self.port,
                               timeout=self.timeout)
            if self.starttls:
                context = getattr(ssl, '_create_unverified_context')(
                    keyfile=self.keyfile,
                    certfile=self.certfile
                )
                self.client.stls(context)
        self.auth_type = auth_type
        if self.auth_type == 'simple':
            self.client.user(self.username)
            self.client.pass_(self.password)
        elif self.auth_type == 'apop':
            self.client.apop(self.username, self.password)
        elif self.auth_type == 'rpop':
            self.client.rpop(self.username)
        else:
            ValueError('access_type is illegal')

    def get_nodes(self, node):
        self.logger.info(node)
        nodes = list()
        resp = self.client.list()
        if re.match(b'^\\+OK', resp[0]):
            for item in resp[1]:
                num, size = item.decode().split()
                node = dict()
                node['num'] = num
                node['size'] = size
                node['type'] = 'file'
                node['name'] = num + '.eml'
                node['path'] = os.path.join(self.username, 'INBOX', node['name'])
                nodes.append(node)
        return nodes

    def get_file(self, node):
        resp = self.client.retr(node['num'])
        if re.match(b'^\\+OK', resp[0]):
            data = b'\n'.join(resp[1])
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
            self.client.quit()


class MultiPOP3(object):

    def __init__(self,
                 host,
                 port,
                 keyfile=None,
                 certfile=None,
                 starttls=False,
                 timeout=None,
                 ssl_wrap=False,
                 auth_type='simple',
                 accounts={}):
        self.host = host
        self.port = port
        self.keyfile = keyfile
        self.certfile = certfile
        self.starttls = boolean(starttls)
        self.timeout = timeout
        self.ssl_wrap = boolean(ssl_wrap)
        self.auth_type = auth_type
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
            kwargs['auth_type'] = self.auth_type
            client = POP3One(**kwargs)
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
            kwargs['auth_type'] = self.auth_type
            client = POP3One(**kwargs)
            return client.get_file(node)


class MultiPOP3Iterator(Iterator):

    def __init__(self, auth, resources):
        self.auth = auth
        self.client = MultiPOP3(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = POP3Dict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = POP3Dict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'POP3'
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
