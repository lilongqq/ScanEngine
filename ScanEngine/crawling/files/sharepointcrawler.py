#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import hashlib
import logging
import time
from xml.sax.saxutils import escape as xml_escape
import io
from functools import partial

import requests
from requests_ntlm import HttpNtlmAuth
from lxml import etree
from dateutil import parser as dateutil_parser
from collections import UserDict

from crawling.interface import Iterator, Client
from common.conntool import weak_cache
from common.functools import gen_key


# SharePoint ServerTemplate 值：101 = 文档库
SERVER_TEMPLATE_DOCLIB = '101'
FSOBJTYPE_FILE = '0'
FSOBJTYPE_FOLDER = '1'


class SharePointClientError(Exception):
    """SharePoint 客户端异常"""
    pass


class SOAPHandler:
    """
    SOAP 请求处理器，参照 se-8-4 的 SOAPHandler 实现。
    使用 requests + NTLM 替代 pycurl，直接构造 SOAP 请求。
    """

    def __init__(self, username, password, domain, site_url, verify_ssl=True):
        self.username = username
        self.password = password
        self.domain = domain
        self.site_url = site_url.rstrip('/')
        self.verify_ssl = verify_ssl
        self.logger = logging.getLogger(self.__class__.__name__)
        self._session = None
        self._init_session()

    def _init_session(self):
        """初始化带 NTLM 认证的 Session"""
        self._session = requests.Session()
        ntlm_user = f'{self.domain}\\{self.username}' if self.domain else self.username
        self._session.auth = HttpNtlmAuth(ntlm_user, self.password)
        # 不在 session 级别设置 Content-Type，避免影响文件下载 GET 请求的 NTLM 握手

    def _send(self, url, soap_action, soap_body):
        """发送 SOAP 请求，返回响应 bytes"""
        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': soap_action
        }
        self.logger.debug('SOAP >> url=%s action=%s', url, soap_action)
        try:
            resp = self._session.post(
                url,
                data=soap_body.encode('utf-8'),
                headers=headers,
                verify=self.verify_ssl,
                timeout=30
            )
            self.logger.debug('SOAP << status=%d url=%s', resp.status_code, url)
            if resp.status_code not in (200, 206):
                self.logger.debug('SOAP error body (first 500 chars): %s', resp.text[:500])
                raise SharePointClientError(
                    f"HTTP {resp.status_code} for {url}: {resp.text[:200]}"
                )
            return resp.content
        except requests.RequestException as e:
            raise SharePointClientError(f"SOAP request failed for {url}: {e}")

    def get_web_collection(self):
        """调 Webs.asmx 的 GetWebCollection，获取子站点列表"""
        url = f'{self.site_url}/_vti_bin/Webs.asmx'
        soap_action = 'http://schemas.microsoft.com/sharepoint/soap/GetWebCollection'
        soap_body = '''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetWebCollection xmlns="http://schemas.microsoft.com/sharepoint/soap/" />
  </soap:Body>
</soap:Envelope>'''
        return self._send(url, soap_action, soap_body)

    def get_list_collection(self):
        """调 Lists.asmx 的 GetListCollection，获取所有列表"""
        url = f'{self.site_url}/_vti_bin/lists.asmx'
        soap_action = 'http://schemas.microsoft.com/sharepoint/soap/GetListCollection'
        soap_body = '''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetListCollection xmlns="http://schemas.microsoft.com/sharepoint/soap/" />
  </soap:Body>
</soap:Envelope>'''
        return self._send(url, soap_action, soap_body)

    def get_list_items(self, list_name, query='', query_options='', view_fields=None, row_limit=''):
        """
        调 Lists.asmx 的 GetListItems，获取列表项

        :param list_name: 列表名称
        :param query: CAML 查询条件 XML
        :param query_options: 查询选项 XML
        :param view_fields: 要返回的字段列表
        :param row_limit: 行数限制
        """
        url = f'{self.site_url}/_vti_bin/lists.asmx'
        soap_action = 'http://schemas.microsoft.com/sharepoint/soap/GetListItems'

        vf_xml = ''
        if view_fields:
            # <viewFields> 是 SOAP 参数名（小写），内层 <ViewFields> 是 CAML 内容（大写）
            fields_inner = ''.join(f'<FieldRef Name="{f}" />' for f in view_fields)
            vf_xml = f'<viewFields><ViewFields>{fields_inner}</ViewFields></viewFields>'
        else:
            vf_xml = '<viewFields><ViewFields /></viewFields>'

        soap_body = f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetListItems xmlns="http://schemas.microsoft.com/sharepoint/soap/">
      <listName>{list_name}</listName>
      <viewName></viewName>
      <query><Query>{query}</Query></query>
      {vf_xml}
      <rowLimit>{row_limit}</rowLimit>
      <queryOptions><QueryOptions>{query_options}</QueryOptions></queryOptions>
      <webID></webID>
    </GetListItems>
  </soap:Body>
</soap:Envelope>'''
        return self._send(url, soap_action, soap_body)

    def get_file_content(self, file_url, offset=0, length=-1):
        """
        通过 HTTP GET 获取文件内容。
        使用独立 Session，避免与 SOAP Session 共用连接导致 NTLM 状态干扰。
        """
        ntlm_user = f'{self.domain}\\{self.username}' if self.domain else self.username
        file_session = requests.Session()
        file_session.auth = HttpNtlmAuth(ntlm_user, self.password)

        try:
            req_headers = {}
            if offset != 0 or length != -1:
                range_end = '' if length == -1 else str(offset + length - 1)
                req_headers['Range'] = f'bytes={offset}-{range_end}'

            resp = file_session.get(
                file_url,
                verify=self.verify_ssl,
                headers=req_headers or None,
                timeout=60,
            )
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            raise SharePointClientError(f"Failed to retrieve file {file_url}: {e}")


class SharePointListsHandler:
    """
    处理 GetListItems 分页，参照 se-8-4 的 SharePointListsHandler 实现。
    """

    # GetListItems 必须显式声明的字段（SP2010 默认视图不包含 FileSizeDisplay）
    REQUIRED_FIELDS = [
        'FileRef',          # 服务器相对路径
        'FileLeafRef',      # 文件名（含扩展名）
        'FSObjType',        # 0=文件 1=文件夹
        'FileSizeDisplay',  # 文件大小（字节），默认视图不返回，必须显式声明
        'Modified',         # 最后修改时间
    ]

    def __init__(self, soap_handler, list_name, query='', query_options='', view_fields=None):
        self.soap_handler = soap_handler
        self.list_name = list_name
        self.query = query
        self.query_options = query_options
        self.view_fields = view_fields if view_fields is not None else self.REQUIRED_FIELDS
        self.cur_page = None
        self.prev_bookmark = ''
        self.logger = logging.getLogger(self.__class__.__name__)

    def _execute_query(self, query_options):
        """执行查询"""
        self.logger.debug(
            'GetListItems >> list_name=%s query_options=%s',
            self.list_name, query_options[:200] if query_options else ''
        )
        try:
            response = self.soap_handler.get_list_items(
                self.list_name,
                self.query,
                query_options,
                view_fields=self.view_fields
            )
            self.cur_page = etree.fromstring(response)
            self.logger.debug('GetListItems << parsed OK, list_name=%s', self.list_name)
        except etree.XMLSyntaxError as e:
            self.logger.warning('XML parse error in GetListItems: %s', e)
            self.cur_page = None

    def has_items(self):
        """当前页是否有数据"""
        if self.cur_page is None:
            return False
        try:
            ns = {
                'sp': 'http://schemas.microsoft.com/sharepoint/soap/',
                'rs': 'urn:schemas-microsoft-com:rowset',
                'z': '#RowsetSchema'
            }
            ic = self.cur_page.find('.//z:row', ns)
            return ic is not None
        except Exception as e:
            self.logger.warning('has_items check failed: %s', e)
            return False

    def get_next_page(self):
        """获取下一页，支持分页"""
        if self.cur_page is None:
            self._execute_query(self.query_options)
            if not self.has_items():
                return []
            return self._parse_rows(self.cur_page)

        try:
            ns = {'rs': 'urn:schemas-microsoft-com:rowset'}
            pos_node = self.cur_page.find('.//rs:data', ns)
            if pos_node is None:
                return []

            # ListItemCollectionPositionNext 是 rs:data 的属性，不是子元素
            next_bookmark = pos_node.get('ListItemCollectionPositionNext', '')
            if not next_bookmark or next_bookmark == self.prev_bookmark:
                return []

            self.prev_bookmark = next_bookmark
            paging_info = f'<Paging ListItemCollectionPositionNext="{xml_escape(next_bookmark)}" />'
            full_options = self.query_options + paging_info

            self._execute_query(full_options)
            if not self.has_items():
                return []
            return self._parse_rows(self.cur_page)

        except Exception as e:
            self.logger.warning('get_next_page failed: %s', e)
            return []

    def _parse_rows(self, root):
        """解析 GetListItems 返回的 rows"""
        ns = {
            'sp': 'http://schemas.microsoft.com/sharepoint/soap/',
            'rs': 'urn:schemas-microsoft-com:rowset',
            'z': '#RowsetSchema'
        }
        rows = []
        for row in root.findall('.//z:row', ns):
            item = {}
            for k, v in row.attrib.items():
                # 去掉 ows_ 前缀
                key = k[4:] if k.startswith('ows_') else k
                item[key] = v
            rows.append(item)
        # 备用解析：直接找 row 元素
        if not rows:
            for row in root.findall('.//row'):
                item = {}
                for k, v in row.attrib.items():
                    key = k[4:] if k.startswith('ows_') else k
                    item[key] = v
                rows.append(item)
        if rows:
            self.logger.debug('_parse_rows: first row keys=%s', list(rows[0].keys()))
        return rows


class Dict(UserDict):
    """用于 Iterator 返回的字典，增强 path/timestamp/diff 字段"""

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data.get('last_write_time', time.time())
        elif item == 'diff':
            return gen_key(
                path=self.data.get('path'),
                last_write_time=self.data.get('last_write_time')
            )
        return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return {'last_write_time', 'path'}.issubset(self.data)
        return item in self.data

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
        return False


class SharePoint(Client):
    """
    SharePoint 扫描客户端，参照 se-8-4 的 SharePointFileBrowser 重构。
    完全不依赖 shareplum，直接构造 SOAP 请求。
    """

    SOAP_NS = {
        'sp': 'http://schemas.microsoft.com/sharepoint/soap/',
        'rs': 'urn:schemas-microsoft-com:rowset',
        'z': '#RowsetSchema'
    }

    def __init__(self,
                 share_point_site,
                 username,
                 password,
                 site_url,
                 version='365',
                 verify_ssl=True,
                 domain=''):
        self.share_point_site = share_point_site
        self.username = username
        self.password = password
        self.site_url = site_url
        if isinstance(verify_ssl, str):
            verify_ssl = verify_ssl.lower() == 'true'
        self.verify_ssl = verify_ssl
        self.domain = domain
        self.version = str(version) if version else '365'
        self.logger = logging.getLogger(self.__class__.__name__)

        # 初始化 SOAP 处理器
        self._soap = SOAPHandler(
            username=username,
            password=password,
            domain=domain,
            site_url=site_url,
            verify_ssl=verify_ssl
        )

        # 获取站点根路径（用于构造文件 URL）
        self._base_url = site_url.rstrip('/')
        self._base_split = self._get_base_split()

        # 缓存
        self._doclib_info = {}    # Title -> {Name(GUID), RootFolder, DefaultViewUrl, ...}
        self._lib_url_roots = {}  # Title -> 服务器相对路径（如 /Shared Documents）

    def _get_base_split(self):
        """从 site_url 提取协议+主机部分，参照 se-8-4 _getBaseSplit"""
        try:
            parts = self._base_url.split('/')
            return parts[0] + '//' + parts[2]
        except Exception:
            return self._base_url

    # ------------------------------------------------------------------
    # 公共 API：get_nodes / get_file / close
    # ------------------------------------------------------------------

    def get_nodes(self, path=''):
        """
        获取节点列表。
        path='' 时返回所有文档库（BaseType=1 或 ServerTemplate=101）。
        path='xxx' 时返回该文档库下的文件/文件夹。
        """
        self.logger.debug('get_nodes called: path=%r', path)
        nodes = []
        if not path:
            # 获取所有文档库
            try:
                for lib in self._get_doclibs():
                    nodes.append({
                        'name': lib['Title'],
                        'children': [],
                        'type': 'dir',
                        'lib_name': lib['Name'],
                        'server_template': lib.get('ServerTemplate', ''),
                    })
                self.logger.debug('get_nodes (root): found %d doclibs', len(nodes))
            except Exception as e:
                self.logger.warning('get_nodes (root) failed: %s', e)
            return nodes

        # 解析 path：剥离可能存在的 //host:port 或 http(s)://host:port 前缀，
        # 再按第一个 '/' 分割出文档库名和子路径。
        # 外部传入的 path 可能是以下格式之一：
        #   "共享文档"                        → lib=共享文档, sub=''
        #   "共享文档/子文件夹"               → lib=共享文档, sub=子文件夹
        #   "//192.168.37.72:45678/共享文档"  → lib=共享文档, sub=''  (UNC 风格)
        #   "http://192.168.37.72:45678/共享文档" → lib=共享文档, sub=''
        parsed = path
        if parsed.startswith('//'):
            # //host:port/lib_name[/sub_path]
            rest = parsed[2:]
            slash = rest.find('/')
            parsed = rest[slash + 1:] if slash >= 0 else rest
        elif '://' in parsed:
            # http(s)://host:port/lib_name[/sub_path]
            base = self._base_url.rstrip('/')
            if parsed.startswith(base):
                parsed = parsed[len(base):]
        parsed = parsed.lstrip('/')

        parts = parsed.split('/', 1)
        lib_name = parts[0]
        sub_path = parts[1] if len(parts) > 1 else ''

        self.logger.debug('get_nodes parsed: raw=%r → lib_name=%r sub_path=%r', path, lib_name, sub_path)

        try:
            # 获取文档库的 URL root（用于构造文件 URL）
            lib_root = self._get_lib_url_root(lib_name)
            self.logger.debug('get_nodes: lib_name=%r lib_root=%r', lib_name, lib_root)

            if not sub_path:
                # 列出该文档库下的根级文件+文件夹
                nodes.extend(self._get_files_and_folders(lib_name, lib_root, ''))
            else:
                # 列出子文件夹内容
                nodes.extend(self._get_files_and_folders(lib_name, lib_root, sub_path))
            self.logger.debug('get_nodes: lib_name=%r returned %d nodes', lib_name, len(nodes))
        except Exception as e:
            self.logger.warning('get_nodes for %s failed: %s', path, e)

        return nodes

    def get_file(self, node):
        """下载文件内容"""
        try:
            file_url = node.get('path')
            content = self._soap.get_file_content(file_url)
            f = io.BytesIO(content)
            return f
        except SharePointClientError as e:
            self.logger.warning('get_file failed: %s', e)
            raise

    def _make_ntlm_session(self):
        """创建带 NTLM 认证的独立 Session"""
        ntlm_user = f'{self.domain}\\{self.username}' if self.domain else self.username
        s = requests.Session()
        s.auth = HttpNtlmAuth(ntlm_user, self.password)
        return s

    def store_file(self, node, f):
        """上传文件到 SharePoint 指定路径（隔离目标）"""
        sep_path = node.get('sepPath', '')
        file_name = node.get('name', '')
        if not sep_path or not file_name:
            raise ValueError('sepPath or name is empty')
        target_url = f'{self._base_split}/{sep_path.strip("/")}/{file_name}'
        session = self._make_ntlm_session()
        resp = session.put(target_url, data=f.read(), verify=self.verify_ssl, timeout=60)
        resp.raise_for_status()

    def empty_file(self, node):
        """将源文件替换为空内容（占位符模式）"""
        file_url = node.get('path', '')
        if not file_url:
            raise ValueError('path is empty')
        session = self._make_ntlm_session()
        resp = session.put(file_url, data=b'', verify=self.verify_ssl, timeout=60)
        resp.raise_for_status()

    def delete_file(self, node):
        """删除 SharePoint 文件"""
        file_url = node.get('path', '')
        if not file_url:
            raise ValueError('path is empty')
        session = self._make_ntlm_session()
        resp = session.delete(file_url, verify=self.verify_ssl, timeout=60)
        resp.raise_for_status()


        pass

    def __bool__(self):
        return True

    def __del__(self):
        pass

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    def _get_doclibs(self):
        """获取所有文档库（ServerTemplate=101），同时填充 _doclib_info 缓存"""
        try:
            self.logger.debug('GetListCollection >> site=%s', self._base_url)
            response = self._soap.get_list_collection()
            root = etree.fromstring(response)

            doclibs = []
            all_lists = root.findall('.//{http://schemas.microsoft.com/sharepoint/soap/}List')
            self.logger.debug('GetListCollection << total lists=%d', len(all_lists))
            for entity in all_lists:
                server_template = entity.get('ServerTemplate', '')
                if server_template == SERVER_TEMPLATE_DOCLIB:
                    info = dict(entity.attrib)
                    doclibs.append(info)
                    title = info.get('Title', '')
                    if title:
                        self._doclib_info[title] = info
                        self.logger.debug(
                            '  doclib: Title=%r  Name(GUID)=%r  RootFolder=%r',
                            title, info.get('Name', ''), info.get('RootFolder', '')
                        )
            self.logger.debug('_get_doclibs: found %d doclibs', len(doclibs))
            return doclibs
        except Exception as e:
            self.logger.warning('_get_doclibs failed: %s', e)
            return []

    def _ensure_doclib_cache(self):
        """确保 _doclib_info 缓存已填充"""
        if not self._doclib_info:
            self._get_doclibs()

    def _get_lib_url_root(self, lib_name):
        """
        获取文档库的服务器相对根路径，使用 RootFolder 属性。
        例：/sites/mysite/Shared Documents
        """
        if lib_name in self._lib_url_roots:
            self.logger.debug('_get_lib_url_root cache hit: %r → %r', lib_name, self._lib_url_roots[lib_name])
            return self._lib_url_roots[lib_name]

        self._ensure_doclib_cache()
        info = self._doclib_info.get(lib_name, {})
        if not info:
            self.logger.warning('_get_lib_url_root: lib_name=%r not found in cache, known libs=%s',
                                lib_name, list(self._doclib_info.keys()))
        root_folder = info.get('RootFolder', '')
        if root_folder:
            self._lib_url_roots[lib_name] = root_folder
            self.logger.debug('_get_lib_url_root: %r → RootFolder=%r', lib_name, root_folder)
            return root_folder

        # RootFolder 缺失时从 DefaultViewUrl 反推：
        # DefaultViewUrl 通常形如 /Lib/Forms/AllItems.aspx 或 /sites/x/Lib/Forms/AllItems.aspx
        # 取 /Forms/ 之前的部分即为库根路径
        default_view_url = info.get('DefaultViewUrl', '')
        if default_view_url:
            idx = default_view_url.lower().find('/forms/')
            if idx > 0:
                derived = default_view_url[:idx]
                self._lib_url_roots[lib_name] = derived
                self.logger.debug(
                    '_get_lib_url_root: %r → derived from DefaultViewUrl=%r → %r',
                    lib_name, default_view_url, derived
                )
                return derived

        # 最终兜底
        fallback = '/' + lib_name
        self.logger.warning(
            '_get_lib_url_root: no RootFolder or DefaultViewUrl for %r, fallback to %r',
            lib_name, fallback
        )
        self._lib_url_roots[lib_name] = fallback
        return fallback

    def _get_files_and_folders(self, lib_name, lib_root, folder_path):
        """
        获取文档库内指定文件夹下的文件和子文件夹。
        参照 se-8-4 _getDocLibSubNodes 和 _getFolderSubNodes。
        """
        nodes = []

        # FileDirRef 存储的路径不含前导斜杠，CAML BeginsWith 用此格式
        query_folder_rel = lib_root.lstrip('/') + ('/' + folder_path if folder_path else '')
        # <Folder> QueryOption 需要服务器相对路径（含前导斜杠）
        query_folder_abs = lib_root + ('/' + folder_path if folder_path else '')
        query_options = ''
        if folder_path:
            query_options = f'<Folder>{query_folder_abs}</Folder>'

        # CAML 查询：FileDirRef 不含前导斜杠
        query = f'''
<Where>
  <BeginsWith>
    <FieldRef Name="FileDirRef" />
    <Value Type="Text">{query_folder_rel}</Value>
  </BeginsWith>
</Where>
<OrderBy>
  <FieldRef Name="FSObjType" />
  <FieldRef Name="FileLeafRef" />
</OrderBy>
'''

        # SP2010 GetListItems 要求传 GUID（Name），不接受显示名（Title）
        self._ensure_doclib_cache()
        list_guid = self._doclib_info.get(lib_name, {}).get('Name', lib_name)
        self.logger.debug(
            '_get_files_and_folders: lib_name=%r guid=%r lib_root=%r folder=%r query_options=%r',
            lib_name, list_guid, lib_root, folder_path, query_options
        )

        handler = SharePointListsHandler(
            self._soap,
            list_guid,
            query=query,
            query_options=query_options
        )

        # 获取第一页
        rows = handler.get_next_page()
        self.logger.debug('_get_files_and_folders: page1 rows=%d guid=%r', len(rows), list_guid)
        # 如果分页支持，继续取后续页
        more_rows = handler.get_next_page()
        page_num = 2
        while more_rows:
            self.logger.debug('_get_files_and_folders: page%d rows=%d guid=%r', page_num, len(more_rows), list_guid)
            rows.extend(more_rows)
            more_rows = handler.get_next_page()
            page_num += 1
        self.logger.debug('_get_files_and_folders: total rows=%d guid=%r', len(rows), list_guid)

        # 用于去重
        seen = set()

        for row in rows:
            try:
                file_ref = self._strip_lookup(row.get('FileRef', ''))
                file_leaf_ref = self._strip_lookup(row.get('FileLeafRef', ''))
                fsobj_type = self._strip_lookup(row.get('FSObjType', ''))

                # 确定类型
                if fsobj_type == FSOBJTYPE_FILE:
                    obj_url = file_ref
                    if obj_url and obj_url not in seen:
                        seen.add(obj_url)

                        # 只取当前层级的直接子文件，过滤掉更深层级的文件
                        obj_url_stripped = obj_url.strip('/')
                        if folder_path:
                            expected_parent = f"{lib_root.lstrip('/')}/{folder_path}"
                        else:
                            expected_parent = lib_root.lstrip('/')
                        # file_ref 去掉文件名后应该等于当前目录
                        file_dir = obj_url_stripped.rsplit('/', 1)[0] if '/' in obj_url_stripped else ''
                        if file_dir != expected_parent:
                            continue

                        # 兼容 FileRef 有/无前导斜杠两种情况（SP 版本差异）
                        file_url = f'{self._base_split}/{obj_url.lstrip("/")}'
                        folder_url_prefix = (
                            f'{self._base_split}{lib_root}/{folder_path}'
                            if folder_path else f'{self._base_split}{lib_root}'
                        )
                        nodes.append({
                            'type': 'file',
                            'name': file_leaf_ref,
                            'path': file_url,
                            'folder': folder_url_prefix,
                            'last_write_time': self._parse_date(row.get('Modified', '')),
                            'size': int(row.get('FileSizeDisplay', 0)) if row.get('FileSizeDisplay') else 0,
                        })
                elif fsobj_type == FSOBJTYPE_FOLDER:
                    # 子文件夹；folder_url 统一去掉前导 /，便于路径比较
                    folder_url = file_ref.strip('/')
                    if folder_url and folder_url not in seen:
                        seen.add(folder_url)
                        lib_root_rel = lib_root.lstrip('/')
                        if folder_path:
                            expected_parent = f"{lib_root_rel}/{folder_path}"
                            if not folder_url.startswith(expected_parent + '/'):
                                continue
                            folder_name = folder_url[len(expected_parent) + 1:]
                        else:
                            if not folder_url.startswith(lib_root_rel + '/'):
                                continue
                            folder_name = folder_url[len(lib_root_rel) + 1:]

                        if folder_name and '/' not in folder_name:  # 只取直接子文件夹
                            if folder_path:
                                node_path = f'{lib_name}/{folder_path}/{folder_name}'
                            else:
                                node_path = f'{lib_name}/{folder_name}'
                            nodes.append({
                                'type': 'dir',
                                'name': folder_name,
                                'path': node_path,
                                'children': [],
                            })
            except Exception as e:
                self.logger.warning('Error parsing row: %s  row=%r', e, row)
                continue

        self.logger.debug('_get_files_and_folders: built %d nodes (lib=%r folder=%r)', len(nodes), lib_name, folder_path)
        return nodes

    @staticmethod
    def _strip_lookup(val):
        """去掉 SharePoint Lookup 的 '1;#' 前缀"""
        s = str(val) if val is not None else ''
        return s.split(';#', 1)[1] if ';#' in s else s

    @staticmethod
    def _parse_date(date_str):
        """解析 SharePoint 日期字符串为 timestamp"""
        if not date_str:
            return time.time()
        try:
            s = SharePoint._strip_lookup(date_str)
            dt = dateutil_parser.parse(s)
            return time.mktime(dt.timetuple())
        except Exception:
            return time.time()


class SharePointBatch(SharePoint):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@weak_cache
class SharePointOne(SharePoint):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class SharePointIterator(Iterator):
    """
    SharePoint 遍历迭代器，参照 se-8-4 的 BFS 遍历逻辑。
    """

    def __init__(self, auth, resources):
        self.auth = auth
        self.client = SharePointBatch(**self.auth)
        self.stack = list()
        self.stack.append(resources)
        self.node = dict()

    def __iter__(self):
        return self

    def __bool__(self):
        return bool(self.stack)

    def __next__(self):
        while len(self.stack) > 0:
            self.node = Dict(self.stack.pop())
            if 'children' in self.node:
                if self.node['children']:
                    children = self.node['children']
                else:
                    children = self.get_nodes(self.node)
                children.reverse()
                self.stack.extend(children)
            elif 'sibling' in self.node:
                if self.node['sibling']:
                    sibling = self.node['sibling']
                else:
                    sibling = self.get_nodes(self.node)
                sibling.reverse()
                self.stack.extend(sibling)
            else:
                return self.node, partial(self.get_file, self.node)
        raise StopIteration

    def get_nodes(self, node):
        path = node.get('path', node.get('name', ''))
        return self.client.get_nodes(path)

    def get_file(self, node):
        f = self.client.get_file(node)
        md5_hash = hashlib.md5(f.read())
        node['md5'] = md5_hash.hexdigest()
        f.seek(0)
        return f

    def add_nodes(self, nodes):
        self.stack.extend(nodes)
