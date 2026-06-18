#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import logging
import os.path
import hashlib
import json
from collections import UserDict
from weakref import WeakValueDictionary
from common.globals import Variables
from crawling.interface import Iterator, Client
from functools import partial
from exchangelib import Credentials, Configuration, Account, Version, version, IMPERSONATION, DELEGATE
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from ex3rd.exchangelib.protocol import NoVerifyHTTPAdapterTLSv1
from exchangelib import Folder
from common.functools import gen_key, boolean


class EWSDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            if 'last_write_time' in self.data:
                return self.data['last_write_time']
            elif 'create_time' in self.data:
                return self.data['create_time']
            else:
                raise KeyError('timestamp')
        elif item == 'diff':
            return gen_key(
                id=self.data.get('id'),
                changekey=self.data.get('changekey')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            if 'last_write_time' in self.data:
                return True
            elif 'create_time' in self.data:
                return True
            else:
                return False
        elif item == 'diff':
            return {'id', 'changekey'}.issubset(self.data)
        else:
            return item in self.data


class EWS(Client):

    def __init__(
            self,
            username,
            password,
            host,
            ews_version,
            access_type,
            primary_smtp_address,
            port=443,
            ssl_wrap=True
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.logger.info(primary_smtp_address)
        self.username = username
        self.password = password
        self.server = host
        self.port = port
        self.ews_version = ews_version
        self.version = Version(getattr(version, self.ews_version))
        self.primary_smtp_address = primary_smtp_address
        self.access_type = access_type
        self.ssl_wrap = ssl_wrap
        if self.ssl_wrap:
            self.service_endpoint = 'https://{server}:{port}/EWS/Exchange.asmx'.format(
                server=self.server,
                port=self.port
            )
            if self.ews_version == 'EXCHANGE_2019':
                BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapter
            else:
                BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapterTLSv1
        else:
            self.service_endpoint = 'http://{server}:{port}/EWS/Exchange.asmx'.format(
                server=self.server,
                port=self.port
            )
        config = Configuration(
            credentials=Credentials(
                username=self.username,
                password=self.password
            ),
            service_endpoint=self.service_endpoint,
            version=self.version,
            max_connections=self.variables.executor['max_workers']
        )
        self.client = Account(
            primary_smtp_address=self.primary_smtp_address,
            access_type=self.access_type,
            autodiscover=False,
            config=config
        )

    def get_nodes(self, node={}):
        nodes = list()
        if node and (
                path := node.get('path')
        ):
            resp = self.client.root.get_folder(Folder(id=node['id']))
            if self.ews_version == 'EXCHANGE_2007':
                for item in resp.all().values(
                        'id',
                        'changekey',
                        'datetime_created'
                ):
                    node = dict()
                    node['type'] = 'file'
                    node['name'] = item['id'] + '.eml'
                    node['path'] = os.path.join(path, node['name'])
                    node['id'] = item['id']
                    node['changekey'] = item['changekey']
                    node['create_time'] = item['datetime_created'].timestamp()
                    node['primary_smtp_address'] = self.primary_smtp_address
                    nodes.append(node)
            else:
                for item in resp.all().values(
                        'id',
                        'changekey',
                        'datetime_created',
                        'last_modified_time'
                ):
                    node = dict()
                    node['type'] = 'file'
                    node['name'] = item['id'] + '.eml'
                    node['path'] = os.path.join(path, node['name'])
                    node['id'] = item['id']
                    node['changekey'] = item['changekey']
                    node['create_time'] = item['datetime_created'].timestamp()
                    node['last_write_time'] = item['last_modified_time'].timestamp()
                    node['primary_smtp_address'] = self.primary_smtp_address
                    nodes.append(node)
        else:
            resp = self.client.msg_folder_root
            for folder in resp.children.folders:
                if folder.folder_class == 'IPF.Note':
                    node = dict()
                    node['type'] = 'file'
                    node['name'] = folder.name
                    node['path'] = os.path.join(
                        self.primary_smtp_address,
                        folder.absolute.split('/', 3)[-1]
                    )
                    node['changekey'] = folder.changekey
                    node['folder_class'] = folder.folder_class
                    node['id'] = folder.id
                    node['children'] = []
                    node['primary_smtp_address'] = self.primary_smtp_address
                    nodes.append(node)
        return nodes

    def get_file(self, node={}):
        items = self.client.fetch(
            ids=[
                (
                    node['id'],
                    node['changekey']
                )
            ],
            only_fields=['mime_content']
        )
        for item in items:
            node['size'] = len(item.mime_content)
            f = io.BytesIO(item.mime_content)
            return f

    def __bool__(self):
        return True

    def __del__(self):
        if self.access_type == DELEGATE:
            self.client.protocol.close()


def get_primary_smtp_address(path):
    path_tuple = path.split('/', 1)
    if len(path_tuple) == 2:
        primary_smtp_address = path_tuple[0]
    else:
        primary_smtp_address = path
    return primary_smtp_address


class MultiEWS(object):

    cache = WeakValueDictionary()

    def __init__(
            self,
            access_type,
            host,
            ews_version,
            username='',
            password='',
            port=443,
            ssl_wrap=True,
            accounts={}
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.access_type = access_type
        self.host = host
        self.port = int(port)
        self.ews_version = ews_version
        self.username = username
        self.password = password
        self.ssl_wrap = boolean(ssl_wrap)
        self.accounts = accounts

    def get_nodes(self, node):
        account = self.get_account(node)
        return account.get_nodes(node)

    def get_file(self, node):
        account = self.get_account(node)
        return account.get_file(node)

    def get_account(self, node):
        kwargs = dict()
        if 'primary_smtp_address' in node:
            primary_smtp_address = node['primary_smtp_address']
        else:
            path = node.get('path', node['name'])
            primary_smtp_address = get_primary_smtp_address(path)
        if self.access_type == IMPERSONATION:
            kwargs['host'] = self.host
            kwargs['port'] = self.port
            kwargs['ews_version'] = self.ews_version
            kwargs['access_type'] = IMPERSONATION
            kwargs['username'] = self.username
            kwargs['password'] = self.password
            kwargs['primary_smtp_address'] = primary_smtp_address
            kwargs['ssl_wrap'] = self.ssl_wrap
        elif self.access_type == DELEGATE:
            kwargs['host'] = self.host
            kwargs['port'] = self.port
            kwargs['ews_version'] = self.ews_version
            kwargs['access_type'] = DELEGATE
            kwargs['username'] = primary_smtp_address
            kwargs['password'] = self.accounts.get(primary_smtp_address)
            kwargs['primary_smtp_address'] = primary_smtp_address
            kwargs['ssl_wrap'] = self.ssl_wrap
        else:
            raise ValueError('access_type is illegal')
        key = json.dumps(
            kwargs,
            ensure_ascii=False,
            indent=4
        )
        if key in self.cache:
            self.logger.info('cache account')
            account = self.cache[key]
        else:
            self.logger.info('create account')
            account = EWS(**kwargs)
            self.cache[key] = account
        return account


class MultiEWSIterator(Iterator):

    def __init__(self, auth, resources):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.client = MultiEWS(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = EWSDict()
        self.account = None

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = EWSDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'EWS'
            if 'children' in self.node:
                if self.node['children']:
                    children = self.node['children']
                else:
                    children = self.get_nodes(self.node)
                children.reverse()
                self.stack.extend(children)
            else:
                return self.node, partial(self.get_file, self.account, self.node)
        raise StopIteration

    def get_nodes(self, node):
        self.logger.info(node)
        if 'primary_smtp_address' in node:
            primary_smtp_address = node['primary_smtp_address']
        else:
            path = node.get('path', node['name'])
            primary_smtp_address = get_primary_smtp_address(path)
        if self.account:
            if self.account.primary_smtp_address != primary_smtp_address:
                self.account = self.client.get_account(node)
        else:
            self.account = self.client.get_account(node)
        return self.account.get_nodes(node)

    def get_file(self, account, node):
        self.logger.info(node)
        f = account.get_file(node)
        self.logger.info('get_file')
        md5 = hashlib.md5()
        md5.update(f.getbuffer())
        node['md5'] = md5.hexdigest()
        return f.getvalue()
