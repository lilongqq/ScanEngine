#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import socket
import time
import re
import io

import logging
from functools import partial
from collections import UserDict

from shareplum import Site
from shareplum import Office365
from shareplum.site import Version
from requests_ntlm import HttpNtlmAuth

SHAREPOINT_VERSION = {
    '2007': Version.v2007,
    '2010': Version.v2010,
    '2013': Version.v2013,
    '2016': Version.v2016,
    '2019': Version.v2019,
    '365': Version.v365,
    'O365': Version.v365,
}

from crawling.interface import Iterator, Client

# SP2010 某些部署的 Sites.asmx/GetSite 会返回 500，但 Folder/File 操作仍可用
# 这里 patch 掉 get_site()，让 Site 初始化不因此中断
import shareplum.site as _sp_site
_orig_get_site = _sp_site._Site2007.get_site

def _safe_get_site(self):
    try:
        return _orig_get_site(self)
    except Exception as e:
        logging.getLogger('SharePoint').warning('get_site() failed, skipped: %s', e)
        return {}

_sp_site._Site2007.get_site = _safe_get_site


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
                 verify_ssl=True,
                 domain=''):
        self.share_point_site = share_point_site
        self.username = username
        self.password = password
        self.site_url = site_url
        self.verify_ssl = verify_ssl
        self.logger = logging.getLogger(self.__class__.__name__)
        self._lib_url_roots = {}  # display_name -> 实际 URL 根路径
        self._field_maps = {}     # lib_name -> {internal_name: display_name}

        # 字符串 version 转枚举
        if isinstance(version, str):
            version = SHAREPOINT_VERSION.get(version, Version.v365)
        self.version = version

        if self.version == Version.v365:
            # SharePoint Online（O365）使用 Cookie 认证
            self.authcookie = Office365(self.share_point_site,
                                   username=self.username, password=self.password).GetCookies()
            self.site = Site(self.site_url,
                        version=self.version, authcookie=self.authcookie, verify_ssl=self.verify_ssl)
        else:
            # 本地部署版本使用 NTLM 认证
            ntlm_user = f'{domain}\\{username}' if domain else username
            auth = HttpNtlmAuth(ntlm_user, self.password)
            self.site = Site(self.site_url,
                        version=self.version, auth=auth, verify_ssl=self.verify_ssl)

    def _fmap(self, lib_name):
        """返回 {内部字段名: 显示名} 映射，结果按 lib_name 缓存。"""
        if lib_name not in self._field_maps:
            lst = self.site.List(lib_name)
            self._field_maps[lib_name] = {
                v['name']: k for k, v in lst._disp_cols.items()
            }
        return self._field_maps[lib_name]

    def _get_lib_url_root(self, lib_name):
        """第一次调用时从列表首条记录推断 URL 根路径并缓存。"""
        if lib_name in self._lib_url_roots:
            return self._lib_url_roots[lib_name]
        try:
            fm = self._fmap(lib_name)
            dir_field = fm.get('FileDirRef')
            lst = self.site.List(lib_name)
            items = lst.GetListItems(fields=[dir_field], row_limit=1)
            if items and dir_field:
                root = self._lv(items[0].get(dir_field, '')).lstrip('/').split('/')[0]
            else:
                root = lib_name
        except Exception:
            root = lib_name
        self._lib_url_roots[lib_name] = root
        return root

    @staticmethod
    def _lv(val):
        """Strip SharePoint Lookup '1;#' prefix."""
        s = str(val) if val is not None else ''
        return s.split(';#', 1)[1] if ';#' in s else s

    def get_nodes(self, path=''):
        nodes = []
        if not path:
            # 动态拉取所有文档库（BaseType=1），不依赖外部传参
            try:
                for col in self.site.GetListCollection():
                    if col.get('BaseType') == 'DocumentLibrary':
                        nodes.append({'name': col['Title'], 'children': []})
            except Exception as e:
                self.logger.warning('GetListCollection failed: %s', e)
            return nodes

        path = path.replace('\\', '/')
        lib_name = path.split('/', 1)[0]

        # 当 path 就是库显示名（如 '共享文档'），换成 URL 根路径做比较
        url_root = self._get_lib_url_root(lib_name)
        if path == lib_name:
            url_path = url_root
        else:
            sub = path.split('/', 1)[1]
            url_path = url_root + '/' + sub

        try:
            fm = self._fmap(lib_name)
            # 用内部字段名反查显示名，不依赖语言
            f_ref  = fm.get('FileRef')
            f_dir  = fm.get('FileDirRef')
            f_fso  = fm.get('FSObjType')
            f_name = fm.get('BaseName')
            f_mod  = fm.get('Last_x0020_Modified')
            fields = [f for f in (f_ref, f_dir, f_fso, f_name, f_mod) if f]
            lst = self.site.List(lib_name)
            items = lst.GetListItems(fields=fields, row_limit=0)
        except Exception as e:
            self.logger.warning('GetListItems failed for %s: %s', lib_name, e)
            return nodes

        for item in items:
            fso_type = self._lv(item.get(f_fso, ''))   if f_fso  else ''
            file_ref = self._lv(item.get(f_ref, '')).lstrip('/') if f_ref  else ''
            dir_ref  = self._lv(item.get(f_dir, '')).lstrip('/').rstrip('/') if f_dir else ''
            name     = item.get(f_name, '')             if f_name else ''
            last_mod = self._lv(item.get(f_mod, ''))   if f_mod  else ''

            if dir_ref != url_path.rstrip('/'):
                continue

            if fso_type == '0':      # 文件
                nodes.append({
                    'path': file_ref,
                    'name': name,
                    'folder': path,
                    'last_write_time': last_mod,
                })
            elif fso_type == '1':    # 子文件夹，name 用显示名前缀
                nodes.append({'name': lib_name + '/' + file_ref.split('/', 1)[1], 'children': []})

        return nodes

    def get_file(self, node):
        if 'folder' in node:
            file_url = self.site_url.rstrip('/') + '/' + node['path'].lstrip('/')
            resp = self.site._session.get(file_url, verify=self.verify_ssl)
            resp.raise_for_status()
            return resp.content

    def close(self):
        ...

    def __bool__(self):
        return True

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

