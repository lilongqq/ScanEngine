# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import hashlib
import logging
import os.path
import requests
from tempfile import SpooledTemporaryFile
from functools import partial
from requests.auth import AuthBase
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from crawling.interface import Iterator, Client
from common.globals import Variables
from exceptions import UDSError
from collections import UserDict
from common.functools import gen_key
from datetime import datetime


class UDSDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['last_write_time']
        elif item == 'diff':
            return gen_key(
                version_id=self.data.get('version_id')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return 'version_id' in self.data
        else:
            return item in self.data


class UDSAuth(AuthBase):

    def __init__(self, ak, sk):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.ak = ak
        self.sk = sk

    def __call__(self, r):
        r.headers['x-uds-access-key-id'] = self.ak
        r.headers['x-uds-access-key-secret'] = self.sk
        return r


class UDS(Client):

    def __init__(
        self,
        ak,
        sk,
        endpoint
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = requests.Session()
        self.ak = ak
        self.sk = sk
        self.endpoint = endpoint
        self.api_version = 'api/v1'
        self.list_url = os.path.join(
            self.endpoint,
            self.api_version,
            'udata/listShareFileProperties'
        )

        self.file_url = os.path.join(
            self.endpoint,
            self.api_version,
            'udata/shareFileDownload'
        )
        self.variables = Variables()
        self.auth = UDSAuth(self.ak, self.sk)
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
        current_page,  # 1
        system_code,  # "99test001"
        deploy_code,  # "99"
        page_size,  # 10
        start_time=None,  # "2023-01-01 00:00:00"
        end_time=None,  # "2025-04-08 00:00:00"
        meta_data_type=None,  # "FJGH_MT_GJBZ"
        file_type=None  # "pdf"
    ):
        nodes = list()
        body = {
            'systemCode': system_code,
            'deployCode': deploy_code,
            'currentPage': current_page,
            'pageSize': page_size,
            'startTime': start_time,
            'endTime': end_time,
            'metaDataType': meta_data_type,
            'fileType': file_type
        }
        resp = self.session.post(
            self.list_url,
            headers={'Content-Type': 'application/json'},
            json=body,
            timeout=300
        )
        self.logger.info(resp.request.url)
        self.logger.info(resp.request.headers)
        self.logger.info(resp.request.body)
        if resp.status_code >= 300:
            self.logger.error(resp.text)
            resp.raise_for_status()
        else:
            self.logger.info(resp.text)
            message = resp.json()
            if message.get('code'):
                raise UDSError(message)
            else:
                data = message.get('data')
                is_truncated = len(data) >= page_size
                for node in data:
                    node['type'] = 'file'
                    node['name'] = node['file']['fullFileName']
                    node['path'] = os.path.join(
                        node['dataId'],
                        str(node['version']),
                        node['name']
                    )
                    node['size'] = node['file']['size']
                    node['version_id'] = '{}_{}'.format(
                        node['dataId'],
                        node['version']
                    )
                    node['create_time'] = datetime.fromisoformat(
                        node['createTime']
                    ).timestamp()
                    node['last_write_time'] = datetime.fromisoformat(
                        node['updateTime']
                    ).timestamp()
                    node['system_code'] = system_code
                    node['deploy_code'] = deploy_code
                    nodes.append(node)
                return is_truncated, nodes

    def get_file(self, node):
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        body = {
            'versionId': node['version_id'],
            'systemCode': node['system_code'],
            'deployCode': node['deploy_code'],
            'documentId': node['dataId']
        }
        resp = self.session.post(
            self.file_url,
            headers={'Content-Type': 'application/json'},
            json=body,
            timeout=300,
            stream=True
        )
        self.logger.info(resp.request.url)
        self.logger.info(resp.request.headers)
        self.logger.info(resp.request.body)
        if resp.status_code >= 300:
            self.logger.error(resp.text)
            resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
        f.seek(0)
        return f

    def __del__(self):
        self.session.close()

    def __bool__(self):
        return True


class UDSIterator(Iterator):
    def __init__(self, auth, resources={}):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.client = UDS(**self.auth)
        self.resources = resources
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
                return self.node, partial(
                    self.get_file,
                    self.node
                )
            elif self.is_truncated:
                is_truncated, nodes = self.client.get_nodes(
                    current_page=self.current_page,
                    **self.resources
                )
                self.is_truncated = is_truncated
                self.current_page += 1
                self.stack.extend(nodes)
            else:
                raise StopIteration

    def get_file(self, node):
        f = self.client.get_file(node)
        md5 = hashlib.file_digest(f, 'md5')
        node['md5'] = md5.hexdigest()
        f.seek(0)
        return f
