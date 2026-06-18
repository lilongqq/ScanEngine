# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import hashlib
import logging
import os.path
import time
import traceback
from tempfile import SpooledTemporaryFile
from functools import partial, wraps
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from crawling.interface import Iterator, Client
from common.conntool import thread_local
from exceptions import UDSError
from collections import UserDict
from common.functools import gen_key


def retry_429(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__qualname__)
        while True:
            try:
                return func(*args, **kwargs)
            except UDSError as e:
                if '429' in str(e):
                    logger.warning('rate limited (429), retrying after 10s')
                    time.sleep(10)
                    continue
                raise
    return wrapper


class UDSDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['updateTime']/1000
        elif item == 'diff':
            return gen_key(
                version_id=self.data.get('version_id')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'updateTime' in self.data
        elif item == 'diff':
            return 'version_id' in self.data
        else:
            return item in self.data


@thread_local
class UDS(Client):

    def __init__(self, ak, sk, endpoint):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.ak = ak
        self.sk = sk
        self.endpoint = endpoint
        self.session = requests.Session()
        self.session.headers.update({
            'x-uds-access-key-id': ak,
            'x-uds-access-key-secret': sk,
        })
        self.session.verify = False
        retries = Retry(total=3, backoff_factor=3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def _post(self, path, body, stream=False):
        resp = self.session.post(
            '{}{}'.format(self.endpoint, path),
            json=body,
            timeout=300,
            stream=stream
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            raise UDSError('[HTTP {} ERROR] {}'.format(resp.status_code, resp.text))
        return resp

    @retry_429
    def get_nodes(
        self,
        current_page,
        system_code,
        deploy_code,
        page_size,
        start_time=None,
        end_time=None,
        meta_data_type=None,
        file_type=None
    ):
        body = {
            'systemCode': system_code,
            'deployCode': deploy_code,
            'currentPage': current_page,
            'pageSize': page_size,
            'startTime': start_time,
            'endTime': end_time,
            'metaDataType': meta_data_type,
            'fileType': file_type,
        }
        self.logger.info(body)
        result = self._post('/api/v1/udata/listShareFileProperties', body).json()
        if result.get('code') != 0:
            raise UDSError(result.get('msg'))
        data = result.get('data') or []
        self.logger.info(len(data))
        nodes = []
        for node in data:
            node['type'] = 'file'
            node['name'] = '{}.{}'.format(node['file']['fileName'], node['file']['suffix'])
            node['path'] = os.path.join(node['dataId'], str(node['version']), node['name'])
            node['size'] = int(node['file']['size'])
            node['version_id'] = '{}_{}'.format(node['dataId'], node['version'])
            node['system_code'] = system_code
            node['deploy_code'] = deploy_code
            self.logger.info(node)
            nodes.append(node)
        is_truncated = len(data) >= page_size
        return is_truncated, nodes

    @retry_429
    def share_file_download(self, version_id, document_id, system_code, deploy_code):
        body = {
            'versionId': version_id,
            'systemCode': system_code,
            'deployCode': deploy_code,
            'documentId': document_id,
        }
        self.logger.info(body)
        resp = self._post('/api/v1/udata/download', body, stream=True)

        f = SpooledTemporaryFile(max_size=10 * 1024 * 1024)
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)
        f.seek(0)
        return f

    def get_file(self, node):
        return self.share_file_download(
            node['version_id'],
            node['dataId'],
            node['system_code'],
            node['deploy_code']
        )

    def __del__(self):
        self.session.close()

    def __bool__(self):
        return True


class UDSIterator(Iterator):
    def __init__(self, auth, resources=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.client = UDS(**self.auth)
        self.resources = resources or {}
        self.current_page = 1
        self.is_truncated = True
        self.stack = list()
        self.node = dict()

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if len(self.stack) > 0:
                self.node = UDSDict(self.stack.pop())
                self.node['auth'] = self.auth
                self.node['cls'] = 'UDS'
                self.logger.info('yield node  name: %s  version_id: %s  diff: %s',
                                 self.node.data.get('name'),
                                 self.node.data.get('version_id'),
                                 self.node['diff'] if 'diff' in self.node else None)
                return self.node, partial(
                    self.get_file,
                    self.node
                )
            elif self.is_truncated:
                try:
                    is_truncated, nodes = self.client.get_nodes(
                        current_page=self.current_page,
                        **self.resources
                    )
                except Exception:
                    self.logger.error(traceback.format_exc())
                    raise
                self.is_truncated = is_truncated
                self.current_page += 1
                self.stack.extend(nodes)
            else:
                raise StopIteration

    def get_file(self, node):
        f = self.client.get_file(node)
        h = hashlib.md5()
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
        node['md5'] = h.hexdigest()
        f.seek(0)
        return f
