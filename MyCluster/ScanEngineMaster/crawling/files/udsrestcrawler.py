# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import hashlib
import logging
import time
import requests
from tempfile import SpooledTemporaryFile
from functools import wraps
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from exceptions import UDSRESTError
from crawling.interface import Iterator, Client
from common.globals import Variables
from common.conntool import request_limit as request_limit_wrapper
from collections import UserDict
from common.functools import gen_key


def retry_429(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__qualname__)
        while True:
            try:
                return func(*args, **kwargs)
            except UDSRESTError as e:
                if int(e.message.get('error_code', 0)) == 429:
                    logger.warning('rate limited (429), retrying after 10s: %s', e.message)
                    time.sleep(10)
                    continue
                raise
    return wrapper


class UDSRESTDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['last_write_time']
        elif item == 'diff':
            return gen_key(
                file_id=self.data.get('file_id'),
                last_write_time=self.data.get('last_write_time')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return {'file_id', 'last_write_time'}.issubset(self.data)
        else:
            return item in self.data


class UDSREST(Client):

    def __init__(
        self,
        ak,
        endpoint
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.session = requests.Session()
        self.session.headers.update(
            {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0'
            }
        )
        self.endpoint = endpoint
        self.ak = ak
        self.session.verify = False
        self.list_url = self.endpoint.rstrip('/') + '/list_files'
        self.file_url = self.endpoint.rstrip('/') + '/get_file'
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

    @retry_429
    def get_nodes(
        self,
        marker,
        last_write_time,
        max_keys=100,
        filters=None
    ):
        body = {
            'auth_code': self.ak,
            'marker': marker,
            'last_write_time': last_write_time,
            'max_keys': max_keys,
            'filters': filters or {}
        }
        resp = self.session.post(
            self.list_url,
            headers={'Content-Type': 'application/json'},
            json=body,
            timeout=60
        )
        self.logger.info(resp.request.url)
        self.logger.info(resp.request.body)
        if resp.status_code >= 300:
            self.logger.error(resp.text)
            resp.raise_for_status()
        else:
            message = resp.json()
            if int(message.get('error_code', 200)) == 200:
                if 'Contents' not in message:
                    message['Contents'] = list()
                for node in message['Contents']:
                    node['type'] = 'file'
                    if 'file_name' in node:
                        node['name'] = node['file_name']
                    elif 'file_id' in node:
                        node['name'] = node['file_id']
                    node['path'] = node['name']
                return message
            else:
                raise UDSRESTError(message)

    @retry_429
    def get_file(self, node):
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        try:
            with self.session.get(
                self.file_url + '/' + str(node['file_id']),
                params={
                    'auth_code': self.ak
                },
                timeout=60,
                stream=True
            ) as resp:
                self.logger.info(resp.request.url)
                self.logger.info(resp.headers)
                if resp.status_code >= 300:
                    self.logger.error(resp.text)
                    resp.raise_for_status()
                else:
                    if 'application/json' in resp.headers.get('Content-Type', 'application/octet-stream'):
                        message = resp.json()
                        raise UDSRESTError(message)
                    else:
                        for chunk in resp.iter_content(chunk_size=8192):
                            f.write(chunk)
                        node['size'] = f.tell()
                        f.seek(0)
                        return f
        except Exception:
            f.close()
            raise

    def __del__(self):
        self.session.close()

    def __bool__(self):
        return True


class UDSRESTIterator(Iterator):

    def __init__(self, auth, resources=None, request_limit=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.request_limit = request_limit or {}
        self.auth = auth
        self.resources = resources or {}
        self.marker = '1'
        self.is_truncated = True
        self.stack = list()
        self.node = UDSRESTDict()
        if self.request_limit:
            self.client = request_limit_wrapper(
                **self.request_limit
            )(
                UDSREST
            )(
                **self.auth
            )
        else:
            self.client = UDSREST(**self.auth)

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if len(self.stack) > 0:
                self.node = UDSRESTDict(self.stack.pop())
                self.node['auth'] = self.auth
                self.node['cls'] = 'UDSREST'
                if not self.node.get('file_id'):
                    continue
                return self.node
            elif self.is_truncated:
                message = self.client.get_nodes(
                    marker=self.marker,
                    **self.resources
                )
                self.is_truncated = message.get('IsTruncated', False)
                if self.is_truncated:
                    self.marker = message.get('NextMarker', self.marker)
                self.stack.extend(message['Contents'])
            else:
                raise StopIteration

    @staticmethod
    def wrapper_node(node):
        return UDSRESTDict(node)

    def get_file(self, node):
        self.logger.info(node)
        f = self.client.get_file(node)
        h = hashlib.md5()
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
        node['md5'] = h.hexdigest()
        f.seek(0)
        return f
