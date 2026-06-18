# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import hashlib
import time
import io
import logging
import os.path
import requests
from datetime import datetime
from functools import partial
from collections import UserDict
from common.functools import gen_key
from requests.auth import AuthBase
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from urllib.parse import quote
from common.globals import Variables
from exceptions import EYouError
from crawling.interface import Iterator, Client


class EYouDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return int(self.data['index_time'])
        elif item == 'diff':
            return gen_key(
                account=self.data.get('account'),
                uid=self.data.get('uid'),
                fid=self.data.get('fid'),
                mid=self.data.get('mid')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'index_time' in self.data
        elif item == 'diff':
            return {'account', 'uid', 'fid', 'mid'}.issubset(self.data)
        else:
            return item in self.data


class SimpleAuth(AuthBase):

    def __init__(
        self,
        url,
        auth_key,
        auth_secret
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.url = url
        self.auth_key = auth_key
        self.auth_secret = auth_secret

    def __call__(self, r):
        timestamp = int(time.time())
        r.headers["Authorization"] = ",".join(
            [
                f'Simple realm={quote(self.url)}',
                f'auth_key={quote(self.auth_key)}',
                f'auth_timestamp={timestamp}',
                f'auth_signature={
                    hashlib.sha1(
                        f'{self.auth_secret}{self.auth_key}{timestamp}'.encode()
                    ).hexdigest()
                }'
            ]
        )
        return r


class EYou(Client):

    def __init__(
        self,
        url,
        auth_type,
        auth_key,
        auth_secret
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.session = requests.Session()
        self.url = url
        self.list_url = os.path.join(self.url, 'api/secrecy/list')
        self.mail_url = os.path.join(self.url, 'api/secrecy/mail')
        self.auth_type = auth_type
        self.auth_key = auth_key
        self.auth_secret = auth_secret
        self.auth = SimpleAuth(self.url, self.auth_key, self.auth_secret)
        self.session.auth = self.auth
        self.session.verify = False
        retries = Retry(
            total=3,
            backoff_factor=3,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_maxsize=self.variables.executor['max_workers']
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def get_nodes(
        self,
        page,
        limit,
        stime,
        etime,
        fid,
        is_del
    ):
        nodes = list()
        self.logger.info(f'stime: {stime}')
        self.logger.info(f'etime: {etime}')
        resp = self.session.get(
            self.list_url,
            params={
                'page': page,
                'limit': limit,
                'stime': int(
                    datetime.fromisoformat(
                        stime
                    ).timestamp()
                ),
                'etime': int(
                    datetime.fromisoformat(
                        etime
                    ).timestamp()
                ),
                'fid': fid,
                'is_del': is_del
            },
            timeout=300
        )
        self.logger.info(resp.request.url)
        self.logger.info(resp.request.headers)
        if resp.status_code >= 300:
            self.logger.error(resp.text)
            resp.raise_for_status()
        else:
            message = resp.json()
            if message.get('code'):
                raise EYouError(message)
            else:
                data = message.get('data')
                curr_page = int(data['curr_page'])
                total_page = int(data['total_page'])
                self.logger.info(
                    f'curr_page: {curr_page}'
                )
                self.logger.info(
                    f'total: {data["total"]}'
                )
                self.logger.info(
                    f'total_page: {total_page}'
                )
                is_truncated = total_page > curr_page
                for node in data['list']:
                    node['type'] = 'file'
                    node['name'] = f'{node["mid"]}.eml'
                    node['path'] = os.path.join(
                        node['account'],
                        node['uid'],
                        node['fid'],
                        node['name']
                    )
                    node['size'] = int(node['size'])
                    self.logger.info(node)
                    nodes.append(node)
                return is_truncated, nodes

    def get_file(self, node):
        f = io.BytesIO()
        resp = self.session.get(
            self.mail_url,
            params={
                'account': node['account'],
                'uid': node['uid'],
                'fid': node['fid'],
                'mid': node['mid']
            },
            timeout=300,
            stream=True
        )
        self.logger.info(resp.request.url)
        self.logger.info(resp.request.headers)
        self.logger.info(resp.headers)
        if resp.status_code >= 300:
            self.logger.error(resp.text)
            resp.raise_for_status()
        else:
            if resp.headers.get('Content-Type') == 'application/octet-stream':
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
                f.seek(0)
                return f
            else:
                message = resp.json()
                raise EYouError(message)

    def __del__(self):
        self.session.close()

    def __bool__(self):
        return True


class EYouIterator(Iterator):
    def __init__(self, auth, resources={}):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.client = EYou(**self.auth)
        self.resources = resources
        self.page = 1
        self.is_truncated = False
        self.folders = self.resources['children']
        self.folder = dict()
        self.stack = list()
        self.node = EYouDict()

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if len(self.stack) > 0:
                self.node = EYouDict(self.stack.pop())
                self.node['auth'] = self.auth
                self.node['cls'] = 'EYOU'
                return self.node, partial(
                    self.get_file,
                    self.node
                )
            elif self.is_truncated:
                is_truncated, nodes = self.client.get_nodes(
                    page=self.page,
                    **self.folder
                )
                self.is_truncated = is_truncated
                self.page += 1
                self.stack.extend(nodes)
            elif len(self.folders) > 0:
                self.folder = self.folders.pop()
                self.is_truncated = True
                self.page = 1
            else:
                raise StopIteration

    def get_file(self, node):
        f = self.client.get_file(node)
        md5 = hashlib.md5()
        md5.update(f.getbuffer())
        node['md5'] = md5.hexdigest()
        return f
