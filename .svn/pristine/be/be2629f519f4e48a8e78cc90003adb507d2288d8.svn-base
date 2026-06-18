#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import hashlib
import logging
import requests
from tempfile import SpooledTemporaryFile
from threading import Lock
from functools import partial
from collections import UserDict
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from crawling.interface import Iterator, Client
from common.functools import gen_key
from crawling.database.dbproxy.dbpcrawler import DBProxyBatch


_DEFAULT_RATE_LIMIT = 500  # 默认每分钟最多调用 DBProxy 500 次
_DEFAULT_CTP_FIELDS = ('ID', 'FILE_NAME', 'FILE_SIZE', 'UPDATE_DATE', 'MIME_TYPE', 'TYPE', 'CATEGORY', 'ACCOUNT_ID')
_DEFAULT_CTP_ID_FIELD = 'ID'
_CTP_FETCH_SIZE = 1000
_CTP_PAGE_SIZE = 100

# 常见 Seeyon 字段名到 node 属性的映射（大写匹配）
_FIELD_ALIAS = {
    'FILE_NAME':   'file_name',
    'FILE_SIZE':   'size',
    'UPDATE_DATE': 'last_write_time',
    'MIME_TYPE':   'mime_type',
    'TYPE':        'ctp_type',
    'CATEGORY':    'category',
    'ACCOUNT_ID':  'account_id',
}


class _RateLimiter:
    """每分钟最多 rate 次调用；rate=0 表示不限速。"""
    def __init__(self, rate):
        self._interval = 60.0 / rate if rate > 0 else 0.0
        self._last = 0.0
        self._lock = Lock()

    def wait(self):
        if not self._interval:
            return
        with self._lock:
            remaining = self._interval - (time.time() - self._last)
            if remaining > 0:
                time.sleep(remaining)
            self._last = time.time()


def _find_table_node(node):
    """递归查找 resources 树中第一个 layer='table' 的节点。"""
    if isinstance(node, dict):
        if node.get('layer') == 'table':
            return node
        for child in node.get('children', []):
            found = _find_table_node(child)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_table_node(item)
            if found:
                return found
    return None


def _row_to_node(fields, row, ctp_id_field):
    """根据实际字段列表和主键列名，将一行数据转为文件节点。"""
    d = dict(zip(fields, row))
    ctp_file_id = d.get(ctp_id_field)
    node = {
        'ctp_file_id':     ctp_file_id,
        'path':            str(ctp_file_id),
        'type':            'file',
        'size':            0,
        'last_write_time': None,
        'mime_type':       None,
        'ctp_type':        None,
        'category':        None,
    }
    for col_name, value in d.items():
        alias = _FIELD_ALIAS.get(col_name.upper())
        if alias and value is not None:
            if alias == 'size':
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    value = 0
            node[alias] = value
    file_name = node.get('file_name') or str(ctp_file_id)
    node['file_name'] = file_name
    node['name'] = file_name
    return node


def _detect_ctp_id_field(col_nodes, explicit=None):
    """从列定义节点中推断主键列名。优先级: 显式指定 > pkColumn/userPk=YES > isPath=YES > 第一列。"""
    if explicit:
        return explicit
    for c in col_nodes:
        if c.get('pkColumn') == 'YES' or c.get('userPk') == 'YES':
            return c['name']
    for c in col_nodes:
        if c.get('isPath') == 'YES':
            return c['name']
    return col_nodes[0]['name'] if col_nodes else _DEFAULT_CTP_ID_FIELD


class SeeyonDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data.get('last_write_time')
        elif item == 'diff':
            if 'ctp_file_id' in self.data:
                return gen_key(ctp_file_id=self.data['ctp_file_id'])
        return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return 'ctp_file_id' in self.data
        return item in self.data


class SeeyonAuth:

    def __init__(self, base_url, userName, password, login_name, callback_url=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.base_url = base_url.rstrip('/')
        self.userName = userName
        self.password = password
        self.login_name = login_name
        self.callback_url = callback_url
        self.token = None
        self.last_refresh_time = 0
        self.token_expires_in = 15 * 60
        self.lock = Lock()

    def get_token(self):
        current_time = time.time()
        if self.token and (current_time - self.last_refresh_time) < (self.token_expires_in - 60):
            return self.token

        with self.lock:
            if self.token and (current_time - self.last_refresh_time) < (self.token_expires_in - 60):
                return self.token
            try:
                resp = requests.post(
                    f'{self.base_url}/seeyon/rest/token/',
                    json={'userName': self.userName, 'password': self.password, 'loginName': self.login_name},
                    headers={'Content-Type': 'application/json'},
                    verify=False,
                    timeout=30
                )
                if resp.status_code == 200:
                    resp_data = resp.json()
                    self.token = resp_data.get('id')
                    if not resp_data.get('bindingUser'):
                        self.logger.error('token response missing bindingUser: loginName=%s may be invalid', self.login_name)
                    self.last_refresh_time = current_time
                    return self.token
                else:
                    self.logger.error('token request failed: %s', resp.status_code)
                    return None
            except Exception as e:
                self.logger.error('token request error: %s', e)
                return None


class SeeyonClient(Client):

    def __init__(self, base_url, userName, password, login_name,
                 callback_url=None, token=None, **_kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.base_url = base_url.rstrip('/')
        self.userName = userName
        self.password = password
        self.login_name = login_name
        self.callback_url = callback_url
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.token = token
        self.auth = SeeyonAuth(base_url, userName, password, login_name, callback_url)

    def get_nodes(self, node):
        resp = self.session.post(
            f'{self.base_url}/seeyon/rest/docs/search',
            json={'archiveID': node.get('archive_id'), 'pageNo': 1, 'pageSize': 20},
            headers=self._get_headers(),
            verify=False
        )
        if resp.status_code == 200:
            return self._parse_doc_list(resp.json())
        return []

    def _parse_doc_list(self, result):
        nodes = []
        for doc in result.get('data', []):
            nodes.append({
                'type':                'file' if not doc.get('is_folder') else 'dir',
                'fr_id':               doc.get('fr_id'),
                'fr_name':             doc.get('fr_name'),
                'fr_size':             doc.get('fr_size'),
                'fr_create_time':      doc.get('fr_create_time'),
                'fr_create_username':  doc.get('fr_create_username'),
                'fr_type':             doc.get('fr_type'),
                'hasAtt':              doc.get('hasAtt'),
            })
        return nodes

    def get_file(self, node):
        ctp_file_id = node.get('ctp_file_id')
        token = node.get('token') or self.token or self.auth.get_token()
        params = {'fileName': node.get('file_name', ''), 'token': token}
        url = f'{self.base_url}/seeyon/rest/attachment/file/{ctp_file_id}'
        # [DEBUG-SEEYON] 下载封装日志
        self.logger.info('[DEBUG-SEEYON][get_file] 下载请求: ctp_file_id=%s, file_name=%s, url=%s, token_present=%s',
                         ctp_file_id, node.get('file_name'), url, bool(token))
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        resp = self.session.get(
            url,
            params=params,
            stream=True,
            verify=False,
            timeout=(10, 300)
        )
        self.logger.info('[DEBUG-SEEYON][get_file] 下载响应: status=%s, headers=%s',
                         resp.status_code, dict(resp.headers))
        if resp.status_code == 200:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
            f.seek(0)
            self.logger.info('[DEBUG-SEEYON][get_file] 下载成功: ctp_file_id=%s, file_name=%s', ctp_file_id, node.get('file_name'))
            return f
        f.close()
        if resp.status_code == 401:
            self.auth.token = None
            self.auth.last_refresh_time = 0
        self.logger.error('[DEBUG-SEEYON][get_file] 下载失败: ctp_file_id=%s, status=%s, resp_body=%s',
                          ctp_file_id, resp.status_code, resp.text[:500])
        raise Exception(f'file download failed: {resp.status_code}')

    def _get_headers(self):
        return {
            'Content-Type': 'application/json',
            'token': self.token or self.auth.get_token() or ''
        }

    def __bool__(self):
        return True

    def __del__(self):
        self.session.close()


class SeeyonBatch(SeeyonClient):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class SeeyonIterator(Iterator):
    """
    auth 字段说明：
      OA 连接:  base_url, userName, password, login_name
      DBProxy:  db_dbtype, db_username, db_password, db_url
    iterator 配置（与 auth 平级）：
      resources:     DBProxy 树结构节点（root→schema→table→column），
                     自动从中提取 schema_name、table_name、字段列表和主键列
      schema_name:   可显式覆盖，不传则从 resources 树中的 table 节点读取
      mysql_table:   可显式覆盖，不传则从 resources 树中的 table 节点读取，默认 CTP_FILE
      ctp_id_field:  文件 ID 列名，不传时自动推断（pkColumn/userPk=YES > isPath=YES > 第一列）
      ctp_where:     可选 WHERE 子句，如 "TYPE=0"
    """

    def __init__(self, auth, resources=None, mysql_table='CTP_FILE',
                 schema_name='', ctp_id_field=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.client = SeeyonBatch(**auth)
        self.stack = list()
        self.node = SeeyonDict()

        # 默认字段配置，从 resources 树中提取后覆盖
        self._ctp_fields = _DEFAULT_CTP_FIELDS
        self._ctp_id_field = ctp_id_field or _DEFAULT_CTP_ID_FIELD

        # [DEBUG-SEEYON] 打印 web 侧传入的原始参数
        self.logger.info('[DEBUG-SEEYON][SeeyonIterator] 收到参数: schema_name=%s, mysql_table=%s, ctp_id_field=%s',
                         schema_name, mysql_table, ctp_id_field)
        self.logger.info('[DEBUG-SEEYON][SeeyonIterator] 收到 resources: %s', resources)

        # 从 resources 树中递归找到 table 节点，提取 schema/table/columns
        if resources:
            table_node = _find_table_node(resources)
            if table_node:
                schema_name = schema_name or table_node.get('schema_name', schema_name)
                mysql_table = table_node.get('table_name', mysql_table)
                col_nodes = [c for c in table_node.get('children', [])
                             if isinstance(c, dict) and c.get('layer') == 'column']
                if col_nodes:
                    self._ctp_fields = tuple(c['name'] for c in col_nodes)
                    self._ctp_id_field = _detect_ctp_id_field(col_nodes, ctp_id_field)

        # 从 auth 中提取 db_* 前缀字段，去掉前缀后作为 DBProxy auth
        dbp_auth = {k[3:]: v for k, v in auth.items() if k.startswith('db_')}
        self._schema = schema_name
        self._table = mysql_table
        self._where = auth.get('ctp_where', None)
        self._db_client = DBProxyBatch(**dbp_auth) if dbp_auth else None
        self._start = 0
        self._db_exhausted = self._db_client is None
        rate = int(auth.get('rate_limit') or _DEFAULT_RATE_LIMIT)
        self._rate_limiter = _RateLimiter(rate)
        self.logger.info('[DEBUG-SEEYON] SeeyonIterator init: schema=%s, table=%s, fields=%s, id_field=%s, rate_limit=%s/min',
                         self._schema, self._table, self._ctp_fields, self._ctp_id_field, rate or '不限速')

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.stack:
                self.node = SeeyonDict(self.stack.pop())
                self.node['auth'] = self.auth
                self.node['cls'] = 'SEEYON'
                if 'children' in self.node:
                    children = self.node['children'] or self.client.get_nodes(self.node)
                    children.reverse()
                    self.stack.extend(children)
                else:
                    return self.node, partial(self.get_file, self.node)
            elif not self._db_exhausted:
                # [DEBUG-SEEYON] DBProxy 查询参数日志
                self.logger.info('[DEBUG-SEEYON][SeeyonIterator] DBProxy query: schema=%s, table=%s, fields=%s, start=%s, page_size=%s, where=%s',
                                 self._schema, self._table, self._ctp_fields, self._start, _CTP_PAGE_SIZE, self._where)
                self._rate_limiter.wait()
                try:
                    data = self._db_client.query(
                        self._schema,
                        self._table,
                        _CTP_FETCH_SIZE,
                        self._start,
                        _CTP_PAGE_SIZE,
                        *self._ctp_fields,
                        where=self._where
                    )
                except Exception as e:
                    self.logger.error('[DEBUG-SEEYON][SeeyonIterator] DBProxy query 失败，终止迭代: %s', e)
                    self._db_exhausted = True
                    raise StopIteration
                rows = data.rows
                self.logger.info('[DEBUG-SEEYON][SeeyonIterator] DBProxy 返回行数=%s, start=%s', len(rows), self._start)
                if rows:
                    self.logger.info('[DEBUG-SEEYON][SeeyonIterator] 第一行样本: %s', rows[0])
                self._start += _CTP_PAGE_SIZE
                if rows:
                    nodes = [_row_to_node(self._ctp_fields, row, self._ctp_id_field) for row in rows]
                    nodes.reverse()
                    self.stack.extend(nodes)
                    if len(rows) < _CTP_PAGE_SIZE:
                        self._db_exhausted = True
                else:
                    self._db_exhausted = True
                    raise StopIteration
            else:
                raise StopIteration

    def __bool__(self):
        return bool(self.stack) or not self._db_exhausted

    def __del__(self):
        if self._db_client:
            try:
                self._db_client.logout()
                self._db_client.close()
            except Exception:
                pass

    def get_nodes(self, node):
        return self.client.get_nodes(node)

    def add_nodes(self, nodes):
        self.stack.extend(nodes)

    def get_file(self, node):
        # [DEBUG-SEEYON] 扫描任务内部调用 get_file 时的 node 内容
        self.logger.info('[DEBUG-SEEYON][SeeyonIterator.get_file] ctp_file_id=%s, file_name=%s',
                         node.data.get('ctp_file_id'), node.data.get('file_name'))
        f = self.client.get_file(node)
        md5 = hashlib.file_digest(f, 'md5')
        node['md5'] = md5.hexdigest()
        f.seek(0)
        return f
