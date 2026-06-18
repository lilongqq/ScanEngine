#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import socket
import time
import re
import io
import os.path
import logging
from functools import partial
from collections import UserDict

from shareplum import Site
from shareplum import Office365
from shareplum.site import Version

SHAREPOINT_VERSION = {
    '2007': Version.v2007,
    '2010': Version.v2010,
    '2013': Version.v2013,
    '2016': Version.v2016,
    '2019': Version.v2019,
    '365': Version.v365,
}

from crawling.interface import Iterator, Client


class Dict(UserDict):

    @property
    def timestamp(self):
        if 'last_write_time' in self.data:
            return self.data.get('last_write_time')
        else:
            return time.time()

    @property
    def key(self):
        return self.data.get('path')

    def __eq__(self, other):
        if self.data.get('path') == other.get('path') \
                and self.data.get('last_write_time') == other.get('last_write_time'):
            return True
        else:
            return False


class SharePoint(Client):

    def __init__(self,
                 share_point_site,
                 username,
                 password,
                 site_url,
                 version=SHAREPOINT_VERSION['365'],
                 verify_ssl=True):
        self.share_point_site = share_point_site
        self.username = username
        self.password = password
        self.site_url = site_url
        self.version = version
        self.logger = logging.getLogger(self.__class__.__name__)
        self.base_dir = 'Shared Documents'

        self.authcookie = Office365(self.share_point_site,
                               username=self.username, password=self.password).GetCookies()
        self.site = Site(self.site_url,
                    version=Version.v365, authcookie=self.authcookie, verify_ssl=True)

    def get_nodes(self, path=''):
        nodes = list()
        if not path:
            root_folder = self.site.Folder(self.base_dir)
            for folder in root_folder.folders:
                node = dict()
                node['name'] = os.path.join(self.base_dir, folder)
                node['children'] = []
                nodes.append(node)
            return nodes
        else:
            folder = self.site.Folder(path)
            for file in folder.files:
                node = dict()
                node['path'] = os.path.join(path, file['Name'])
                node['name'] = file['Name']
                node['folder'] = path
                nodes.append(node)

            for folder in folder.folders:
                node = dict()
                node['name'] = os.path.join(path, folder)
                node['children'] = []
                nodes.append(node)
            return nodes


    def get_file(self, node):
        if 'folder' in node:
            folder = self.site.Folder(node['folder'])
            return folder.get_file(node['name'])

    def close(self):
        ...

    def __bool__(self):
        ...

    def __del__(self):
        ...


class SharePointBatch(SharePoint):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class SharePointOne(SharePoint):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class SharePointIterator(Iterator):
    def __init__(self, auth, resources):
        self.auth = auth
        self.client = SharePointBatch(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = dict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = Dict(self.stack.pop())
            if 'children' in self.node:
                if self.node['children']:
                    children = self.node['children']
                else:
                    children = self.get_nodes(self.node)
                self.stack.extend(children)
            else:
                return self.node, partial(self.get_file, self.node)
        raise StopIteration

    def get_nodes(self, node):
        path = node.get('path', node['name'])
        return self.client.get_nodes(path)

    def get_file(self, node):
        return self.client.get_file(node)

if __name__ == '__main__':
    f = {
            'auth': {
                    'share_point_site':'https://fengx.sharepoint.com/',
                    'version':'365',
                    'username':'fengx@fengx.onmicrosoft.com',
                    'password': 'Python0618',
                    'site_url': 'https://fengx.sharepoint.com/sites/site2'
            },
            'resources': {
                'name': '',
                'children': [
                    {'children': [], 'comments': '', 'isSpecial': False, 'name': 'Shared Documents\\test'},
                ]
            }
    }
    ii = SharePointIterator(**f)
    for i in ii:
        print(i)
    s = SharePoint(**f['auth'])
    print(s.get_nodes('Shared Documents\\test'))