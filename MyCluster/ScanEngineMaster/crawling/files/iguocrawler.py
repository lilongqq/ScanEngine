# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import logging
import os.path
import io
import json
import requests
from threading import Lock
from base64 import b64decode
from functools import partial
from datetime import datetime
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from requests.auth import AuthBase
from collections import UserDict
from common.functools import gen_key
from common.globals import Variables
from crawling.interface import Iterator, Client
from exceptions import IguoError


class IguoDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['send_time']
        elif item == 'diff':
            if 'md5' in self.data:
                return self.data['md5']
            elif 'msgid' in self.data:
                return gen_key(
                    msgid=self.data.get('msgid')
                )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'send_time' in self.data
        elif item == 'diff':
            if 'md5' in self.data:
                return True
            elif 'msgid' in self.data:
                return True
            else:
                return False
        else:
            return item in self.data


class IguoAuth(AuthBase):

    def __init__(self, url, corpid, secret):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=3,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.lock = Lock()
        self.url = url
        self.corpid = corpid
        self.secret = secret
        self.expires_in = 7200
        self.last_refresh_time = 0
        self.token = None
        self.refresh_token(time.time())

    def __call__(self, r):
        current_time = time.time()
        if (current_time - self.last_refresh_time) > (self.expires_in - 15):
            self.refresh_token(current_time)
        r.prepare_url(r.url, params={'access_token': self.token})
        return r

    def refresh_token(self, current_time):
        try:
            self.lock.acquire()
            if (current_time - self.last_refresh_time) > 60:
                resp = self.session.get(
                    self.url,
                    params={'corpid': self.corpid, 'corpsecret': self.secret},
                    headers={'Content-Type': 'application/json'},
                    verify=False
                )
                if resp.status_code >= 300:
                    resp.raise_for_status()
                else:
                    message = resp.json()
                    if message.get('errcode', 0):
                        raise IguoError(message)
                    else:
                        self.token = message.get('access_token', None)
                        self.expires_in = message.get('expires_in', 0)
                        self.last_refresh_time = time.time()
                        self.logger.info(self.expires_in)
                        self.logger.info(self.last_refresh_time)
            else:
                self.logger.info('the token is already refresh')
        except Exception:
            raise
        finally:
            self.lock.release()


class Iguo(Client):

    def __init__(
        self,
        url,
        corpid,
        secret,
        pkey
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.session = requests.Session()
        self.base_url = url
        self.auth_url = os.path.join(
            self.base_url,
            'cgi-bin/gettoken'
        )
        self.logs_url = os.path.join(
            self.base_url,
            'cgi-bin/corp/get_log_list'
        )
        self.data_url = os.path.join(
            self.base_url,
            'cgi-bin/corp/get_log_media_data'
        )
        self.corpid = corpid
        self.secret = secret
        self.pkey = serialization.load_pem_private_key(
            pkey.encode(),
            password=None
        )
        self.auth = IguoAuth(self.auth_url, self.corpid, self.secret)
        self.session.auth = self.auth
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

    def get_logs(self, feature_id, start_time, end_time, start=0, limit=1000):
        body = {
            'feature_id': feature_id,
            'start_time': start_time,
            'end_time': end_time,
            'start': start,
            'limit': limit
        }
        resp = self.session.post(
            self.logs_url,
            headers={'Content-Type': 'application/json'},
            json=body
        )
        self.logger.info(resp.request.url)
        self.logger.info(resp.request.headers)
        self.logger.info(resp.request.body)
        if resp.status_code >= 300:
            self.logger.error(resp.text)
            resp.raise_for_status()
        else:
            message = resp.json()
            if message.get('errcode', 0):
                raise IguoError(message)
            else:
                log_list = message.get('log_list', [])
                is_truncated = len(log_list) >= limit
                logs = list()
                for log_encrypted in log_list:
                    log = self.log_decrypt(log_encrypted)
                    logs.append(log)
                return logs, is_truncated

    def get_data(self, fileid, start_index=0, block_size=0):
        body = {
            'fileid': fileid,
            'start_index': start_index,
            'block_size': block_size
        }
        resp = self.session.post(
            self.data_url,
            headers={'Content-Type': 'application/json'},
            json=body
        )
        self.logger.info(resp.request.url)
        self.logger.info(resp.request.headers)
        self.logger.info(resp.request.body)
        if resp.status_code >= 300:
            self.logger.error(resp.text)
            resp.raise_for_status()
        else:
            message = resp.json()
            if message.get('errcode', 0):
                raise IguoError(message)
            else:
                data = self.data_decrypt(message)
                return data, message.get('is_finished', True)

    def __bool__(self):
        return True

    def __del__(self):
        self.session.close()

    def rsa_decrypt(self, cipherdata):
        plaindata = self.pkey.decrypt(
            ciphertext=cipherdata,
            padding=padding.PKCS1v15()
        )
        return plaindata

    def aes_decrypt(self, key, cipherdata):
        cipher = Cipher(algorithms.AES128(key), modes.CBC(cipherdata[0:16]))
        decryptor = cipher.decryptor()
        unpadder = PKCS7(algorithms.AES128.block_size).unpadder()
        plaindata_padded = decryptor.update(
            cipherdata[16:]
        ) + decryptor.finalize()
        plaindata = unpadder.update(plaindata_padded) + unpadder.finalize()
        return plaindata[:-8]

    @staticmethod
    def json_loads(s):
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            decoder = json.JSONDecoder(strict=False)
            s = s.decode()
            return decoder.decode(s)

    def log_decrypt(self, log_encrypted):
        key = self.rsa_decrypt(b64decode(log_encrypted.get('enc_key')))
        log = self.aes_decrypt(key, b64decode(log_encrypted.get('enc_data')))
        return self.json_loads(log)

    def data_decrypt(self, data_encrypted):
        key = self.rsa_decrypt(b64decode(data_encrypted.get('enc_key')))
        data = self.aes_decrypt(key, b64decode(data_encrypted.get('enc_data')))
        return data

    def get_file(self, node):
        f = io.BytesIO()
        start_index = 0
        block_size = 5242880
        while True:
            data, is_finished = self.get_data(
                node['fileid'],
                start_index=start_index,
                block_size=block_size
            )
            f.write(data)
            start_index += block_size
            if start_index >= int(node['size']):
                self.logger.info(
                    'is_finished is ignore {}'.format(is_finished)
                )
                break
        f.seek(0)
        return f


class IguoIterator(Iterator):

    def __init__(self, auth, resources={}):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.client = Iguo(**self.auth)
        self.stack = list()
        if resources:
            self.msg_types = set(resources['msg_types'])
            self.stack.append(resources)
        self.file_msg = dict()
        self.log_list = list()
        self.node = dict()
        self.start = 0
        self.is_truncated = False

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.file_msg and self.file_msg['content']['file_msg']:
                node = IguoDict(self.file_msg)
                node['type'] = 'file'
                del node['content']
                node.update(self.file_msg['content']['file_msg'].pop())
                node['path'] = os.path.join(
                    node.get('fileid', ''),
                    node.get('name', '')
                )
                node['feature_id'] = self.node['feature_id']
                node['last_write_time'] = node['send_time']
                node['auth'] = self.auth
                node['cls'] = 'IGUO'
                return node
            elif len(self.log_list) > 0:
                log = self.log_list.pop()
                msg_type = log.get('msg_type', 0)
                if msg_type not in self.msg_types:
                    continue
                if msg_type == 2:
                    self.file_msg = log
                elif msg_type == 3:
                    self.logger.info('ignore voice message')
                elif msg_type == 4:
                    self.logger.info('ignore vedio message')
                elif msg_type == 13:
                    self.file_msg = log
                elif msg_type == 14:
                    self.logger.info('ignore emotion message')
                else:
                    self.logger.info(msg_type)
                    self.logger.info('ignore text message')
            elif self.is_truncated:
                try:
                    logs, is_truncated = self.client.get_logs(
                        int(
                            self.node['feature_id']
                        ),
                        int(
                            datetime.fromisoformat(
                                self.node['start_time']
                            ).timestamp()
                        ),
                        int(
                            datetime.fromisoformat(
                                self.node['end_time']
                            ).timestamp()
                        ),
                        start=self.start
                    )
                    self.log_list.extend(logs)
                    self.is_truncated = is_truncated
                    self.start += len(logs)
                except IguoError as e:
                    if e.message.get('errcode', 0) in (40014, 42001):
                        self.client.auth.refresh_token(time.time())
                    else:
                        self.is_truncated = False
                except Exception:
                    self.is_truncated = False
            elif len(self.stack) > 0:
                self.node = self.stack.pop()
                if 'children' in self.node:
                    if self.node['children']:
                        children = self.node['children']
                    else:
                        children = self.get_nodes(self.node)
                    children.reverse()
                    self.stack.extend(children)
                else:
                    self.logger.info('start fetch data:')
                    self.logger.info(self.node)
                    self.start = 0
                    self.is_truncated = True
            else:
                raise StopIteration

    @staticmethod
    def wrapper_node(node):
        return IguoDict(node)

    def get_file(self, node):
        f = self.client.get_file(node)
        return f
