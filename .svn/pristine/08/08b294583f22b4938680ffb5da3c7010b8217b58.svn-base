#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import hashlib
import logging
import os.path
import requests
import time
import json
import threading
from functools import partial
from collections import UserDict
from collections.abc import MutableMapping
from functools import gen_key, singledispatchmethod
from requests.auth import AuthBase
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from common.globals import Variables
from common.crypto import Asymmetric
from crawling.interface import Iterator, Client

# 尝试导入存储相关模块
try:
    from dataprocessing.storage import MinioStorage
    from mq import Producer
    STORAGE_AVAILABLE = True
    STORAGE_IMPORT_ERROR = None
except ImportError as e:
    STORAGE_AVAILABLE = False
    STORAGE_IMPORT_ERROR = str(e)
    MinioStorage = None
    Producer = None

# 自定义异常类
class AiShuException(Exception):
    """爱数网盘基础异常"""
    pass

class AuthenticationException(AiShuException):
    """
    爱数网盘认证失败异常
    """
    pass

class DownloadException(AiShuException):
    """下载相关异常"""
    pass

class StorageException(AiShuException):
    """存储相关异常"""
    pass

class AiShuDict(UserDict):
    """
    爱数网盘数据字典类
    基于5.0版本API实现，遵循现有设计模式
    """
    
    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data.get('last_write_time', 0)
        elif item == 'diff':
            return gen_key(
                path=self.data.get('path'),
                last_write_time=self.data.get('last_write_time')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return {'last_write_time', 'path'}.issubset(self.data)
        else:
            return item in self.data


class AiShuAuth(AuthBase):
    """
    爱数网盘认证类
    基于5.0版本的认证机制：用户名/密码获取userid和tokenid
    """
    def __init__(self, url, username, password, pkey=None, verify_ssl=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.url = url.rstrip('/')
        self.username = username
        self.password = password
        self.pkey = pkey  # 用户上传的公钥内容
        self.userid = None
        self.tokenid = None
        self._lock = threading.Lock()
        # 初始化session对象
        self.session = requests.Session()
        # 处理verify_ssl参数的不同类型（布尔值、字符串）
        original_verify_ssl = verify_ssl
        if isinstance(verify_ssl, str):
            self.session.verify = verify_ssl.lower() == 'true'
        else:
            self.session.verify = verify_ssl
        # 记录接收到的参数
        self.logger.info(f"AiShuAuth初始化，接收到参数：url={self.url}, username={self.username}, pkey={'已提供' if self.pkey else '未提供'}, verify_ssl={original_verify_ssl}(处理后={self.session.verify})")
        # 记录接收到的认证参数
        self.logger.info(f"接收到认证参数：url={self.url}, username={self.username}, pkey={'已提供' if self.pkey else '未提供'}, verify_ssl={verify_ssl}(处理后={self.session.verify})")
        # 设置请求重试策略
        retries = Retry(total=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def authenticate(self):
        """
        认证方法
        基于5.0版本API：{protocol}://{ip}:9998/v1/auth1?method=getnew
        密码需要RSA加密，返回userid和tokenid
        """
        with self._lock:  # 确保线程安全
            try:
                self.logger.info("开始爱数网盘5.0版本认证...")
                self.logger.info(f"认证URL：{self.url}")
                self.logger.info(f"用户名：{self.username}")
                self.logger.info(f"是否使用用户上传公钥：{'是' if self.pkey else '否'}")
                self.logger.info(f"SSL验证设置：{self.session.verify}")
                
                # 解析协议和主机
                protocol = "https" if self.url.startswith("https://") else "http"
                base_host = self.url.replace("https://", "").replace("http://", "")
                
                # 使用正确的认证端点、端口和协议
                auth_url = f"{protocol}://{base_host}:9998/v1/auth1?method=getnew"
                
                # RSA加密密码（简化版本，实际应使用真正的RSA公钥加密）
                encrypted_password = self._encrypt_password_rsa(self.password)
                
                auth_data = {
                    "account": self.username,
                    "password": encrypted_password
                }
                
                self.logger.debug(f"认证参数: account={auth_data['account']}, password=*****")
                
                # 设置请求头
                headers = {"content-type": "application/json"}
                
                resp = self.session.post(
                    auth_url,
                    json=auth_data,
                    headers=headers,
                    timeout=300
                )
                
                resp.raise_for_status()  # 自动处理HTTP错误
                
                data = resp.json()
                
                # 检查认证响应
                if "userid" in data and "tokenid" in data:
                    # 保存认证信息
                    self.userid = data.get("userid")
                    self.tokenid = data.get("tokenid")
                    self.logger.info(f"认证成功: userid={self.userid[:10]}..., tokenid={self.tokenid[:10]}...")
                    return True
                else:
                    self.logger.error(f"认证失败: 响应缺少userid或tokenid字段 - {data}")
                    raise AuthenticationException(f"爱数网盘认证失败: 响应缺少必要字段")
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"认证网络异常: {str(e)}")
                raise AuthenticationException(f"爱数网盘认证网络异常: {str(e)}") from e
            except json.JSONDecodeError as e:
                self.logger.error(f"认证响应解析异常: {str(e)}")
                raise AuthenticationException(f"爱数网盘认证响应解析异常: {str(e)}") from e
            except Exception as e:
                self.logger.error(f"认证异常: {str(e)}")
                raise AuthenticationException(f"爱数网盘认证异常: {str(e)}") from e
    
    def __del__(self):
        """
        资源清理
        """
        if hasattr(self, 'session'):
            self.session.close()
    
    def _encrypt_password_rsa(self, password):
        """
        RSA密码加密
        参考爱数5.0版本Java/C示例的配置方式，优化公钥路径加载
        """
        try:
            # 1. 优先使用用户上传的公钥内容（与SFTP保持一致的字段名pkey）
            if self.pkey:
                self.logger.info("使用用户上传的RSA公钥进行加密")
                self.logger.debug(f"公钥内容长度: {len(self.pkey)} 字符")
                # 直接使用用户上传的公钥内容创建Asymmetric实例
                crypto = Asymmetric(public_key_content=self.pkey)
                encrypted_password = crypto.encrypt(password)
                self.logger.debug(f"使用用户上传公钥的RSA加密成功，加密后长度: {len(encrypted_password)} 字符")
                return encrypted_password
            
            self.logger.debug("未提供用户上传的公钥，尝试从其他来源获取")
            
            # 2. 尝试从环境变量获取公钥路径（系统级配置）
            public_key_path = os.environ.get('AISHU_PUBLIC_KEY_PATH')
            if public_key_path:
                self.logger.debug(f"从环境变量获取到公钥路径: {public_key_path}")
            
            # 3. 尝试从Variables实例的配置中获取（应用级配置）
            if not public_key_path:
                try:
                    # 使用Variables单例获取配置
                    variables = Variables()
                    # 支持点号分隔的多层级配置路径，兼容Java示例的配置风格
                    config_section = variables.conf
                    self.logger.debug(f"当前配置根节点类型: {type(config_section).__name__}")
                    
                    for key in 'aishu.encryption.public_key_path'.split('.'):
                        if isinstance(config_section, dict) and key in config_section:
                            config_section = config_section[key]
                            self.logger.debug(f"获取配置层级 {key}: {config_section}")
                        else:
                            self.logger.debug(f"配置层级 {key} 不存在")
                            config_section = None
                            break
                    public_key_path = config_section
                except Exception as config_err:
                    self.logger.debug(f"从配置中获取公钥路径失败: {str(config_err)}")
            
            # 4. 使用默认路径（应用内配置）
            if not public_key_path:
                # 参考Java示例，使用应用根目录下的certs文件夹
                variables = Variables()
                public_key_path = os.path.join(variables.certs, 'aishu_public_key.pem')
                self.logger.debug(f"使用默认公钥路径: {public_key_path}")
            
            self.logger.info(f"使用RSA公钥路径: {public_key_path}")
            
            # 检查公钥文件是否存在
            if os.path.exists(public_key_path):
                file_size = os.path.getsize(public_key_path)
                self.logger.debug(f"公钥文件大小: {file_size} 字节")
            else:
                self.logger.warning(f"公钥文件不存在，但仍尝试加载: {public_key_path}")
            
            # 创建Asymmetric实例并使用公钥加密
            crypto = Asymmetric(public_key_path)
            encrypted_password = crypto.encrypt(password)
            self.logger.debug(f"密码RSA加密成功，加密后长度: {len(encrypted_password)} 字符")
            return encrypted_password
        except FileNotFoundError as e:
            self.logger.error(f"RSA公钥文件未找到: {public_key_path} - {str(e)}")
            # 加密失败时回退到base64编码作为临时方案
            import base64
            fallback_enc = base64.b64encode(password.encode('utf-8')).decode('utf-8')
            self.logger.warning(f"RSA加密失败，回退到base64编码: {fallback_enc[:20]}...")
            return fallback_enc
        except Exception as e:
            self.logger.error(f"RSA加密失败: {str(e)}", exc_info=True)
            # 加密失败时回退到base64编码作为临时方案
            import base64
            fallback_enc = base64.b64encode(password.encode('utf-8')).decode('utf-8')
            self.logger.warning(f"RSA加密失败，回退到base64编码: {fallback_enc[:20]}...")
            return fallback_enc
    
    def is_authenticated(self) -> bool:
        """检查是否已认证"""
        return self.userid is not None and self.tokenid is not None
    
    def __call__(self, r):
        """
        Requests认证回调
        当认证信息不存在或过期时，自动重新认证
        """
        try:
            if not self.is_authenticated():
                self.logger.warning("认证信息不存在，尝试重新认证")
                self.authenticate()
            
            # 添加认证头
            r.headers["userid"] = self.userid
            r.headers["tokenid"] = self.tokenid
            self.logger.debug(f"添加认证头: userid={self.userid[:10]}..., tokenid={self.tokenid[:10]}...")
        except Exception as e:
            self.logger.error(f"认证回调异常: {str(e)}")
        return r


class AiShu(Client):
    """
    爱数网盘客户端
    基于5.0版本API实现，严格遵循现有Client基类设计规范
    """
    
    def __init__(self, url, username, password, pkey=None, verify_ssl=False, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.session = requests.Session()
        
        # 5.0版本配置
        self.url = url.rstrip('/')
        self.username = username
        self.password = password
        self.pkey = pkey
        
        # 处理verify_ssl参数的不同类型（布尔值、字符串）
        original_verify_ssl = verify_ssl
        if isinstance(verify_ssl, str):
            ssl_verify = verify_ssl.lower() == 'true'
        else:
            ssl_verify = verify_ssl
        # 记录接收到的参数
        self.logger.info(f"AiShu客户端初始化，接收到参数：url={self.url}, username={self.username}, pkey={'已提供' if self.pkey else '未提供'}, verify_ssl={original_verify_ssl}(处理后={ssl_verify})")
        
        # 认证器
        self.auth = AiShuAuth(self.url, username, password, pkey, ssl_verify)
        self.session.auth = self.auth
        self.session.verify = ssl_verify
        # 记录客户端初始化参数
        self.logger.info(f"AiShu客户端初始化完成，参数：url={self.url}, username={self.username}, verify_ssl={verify_ssl}(处理后={ssl_verify})")
        
        # HTTP适配器配置
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
    
    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        """
        获取文件节点列表
        支持两种调用方式：
        1. 前端调用：get_nodes(node) - 返回目录层级结构
        2. 前端调用：get_nodes(path) - 返回目录层级结构
        
        基于5.0版本API：http://{ip}:9123/v1/entrydoc?method=get&userid=...&tokenid=...
        """
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        """
        处理节点对象参数，返回目录层级结构（树状）
        用于前端第一次请求，获取树形目录结构
        """
        self.logger.info(f"调用get_nodes(node)，节点信息: {json.dumps(node, ensure_ascii=False)[:100]}...")
        return self._get_directory_structure(node)

    @get_nodes.register(str)
    def _(self, path):
        """
        处理路径字符串参数，返回目录层级结构（树状）
        用于前端第一次请求，获取树形目录结构
        """
        self.logger.info(f"调用get_nodes(path)，路径: {path}")
        return self._get_directory_structure(path)

    def get_nodes_full(self, page=1, limit=100, folder_id='root', 
                      start_time=None, end_time=None, file_type=None):
        """
        获取完整节点列表（分页）
        用于内部调用，获取完整的节点列表
        
        基于5.0版本API：http://{ip}:9123/v1/entrydoc?method=get&userid=...&tokenid=...
        """
        # 记录get_nodes_full方法调用参数
        self.logger.info(f"调用get_nodes_full方法，参数：page={page}, limit={limit}, folder_id={folder_id}, start_time={start_time}, end_time={end_time}, file_type={file_type}")
        
        # 处理内部调用：获取完整节点列表
        nodes = list()
        
        # 执行认证
        if not self.auth.authenticate():
            raise Exception("爱数网盘认证失败")
            
        try:
            # 爱数5.0版本API - 使用正确的端口和端点
            # 根据url的协议动态选择http或https
            protocol = "https" if self.url.startswith("https://") else "http"
            # 提取纯IP/域名，移除协议前缀
            base_host = self.url.replace("https://", "").replace("http://", "")
            api_url = f"{protocol}://{base_host}:9123/v1/entrydoc"
            
            # 参数通过URL查询参数传递
            params = {
                "method": "get",
                "userid": self.auth.userid,
                "tokenid": self.auth.tokenid
            }
            
            # 添加folder_id参数（始终添加，根目录使用"root"）
            params["folder_id"] = folder_id or "root"
            
            # 添加分页参数
            params["page"] = page
            params["limit"] = limit
            
            # 添加时间过滤参数（如果提供）
            if start_time:
                params["start_time"] = start_time
            if end_time:
                params["end_time"] = end_time
            
            self.logger.info(f'API请求URL: {api_url}, 参数: {params}')

            resp = self.session.get(api_url, params=params, timeout=300)
            
            if resp.status_code >= 300:
                self.logger.error(f"API错误: {resp.status_code} - {resp.text}")
                resp.raise_for_status()
            
            data = resp.json()
            
            # 解析爱数5.0版本的响应格式
            docinfos = data.get("docinfos", [])
            # 爱数5.0 API可能返回total_count字段，如果没有则使用len(docinfos)
            total_count = data.get("total_count", len(docinfos))

            # 计算是否还有更多数据
            is_truncated = (page * limit) < total_count
            
            for doc_info in docinfos:
                # 构造节点信息 - 严格遵循现有格式
                doc_type = doc_info.get('doctype')
                doc_name = doc_info.get('docname', '')
                
                # 构建路径 - 与其他服务保持一致，使用完整路径格式
                # 如果是根目录，直接使用文件名作为路径
                if folder_id == 'root':
                    path = f"/{doc_name}"
                else:
                    # 获取父目录的路径信息
                    # 为了避免额外的API调用，我们根据节点关系构建路径
                    # 先获取当前文件夹的路径信息
                    # 这里使用folder_id作为父路径的一部分，确保路径唯一性
                    path = f"/{folder_id}/{doc_name}"
                
                node = {
                    'type': 'file' if doc_type != 'dir' else 'dir',
                    'name': doc_name,
                    'path': path,  # 使用一致的路径格式
                    'size': doc_info.get('size', 0),  # 使用API返回的大小
                    'file_id': doc_info.get('docid', ''),
                    'version': '1',
                    'create_time': doc_info.get('create_time'),
                    'update_time': doc_info.get('modified_time'),
                    'last_write_time': doc_info.get('modified_time'),
                    'folder_id': folder_id or 'root',
                    'gns_path': doc_info.get('docid', ''),
                    'docid': doc_info.get('docid', '')  # 添加原始docid字段
                }
                
                # 应用文件类型过滤（如果提供）
                if file_type and node['type'] == 'file':
                    # 获取文件扩展名
                    file_ext = os.path.splitext(node['name'])[1].lower().lstrip('.')
                    if file_ext not in file_type:
                        self.logger.debug(f"文件类型过滤: 跳过文件 {node['name']} (扩展名: {file_ext})")
                        continue  # 跳过不符合类型的文件
                
                self.logger.debug(f"处理文件: {node['name']} (id: {node['file_id']}, 路径: {node['path']})")
                nodes.append(node)
            
            self.logger.info(f"获取节点成功，返回 {len(nodes)} 个节点，总计 {total_count} 个节点")
            return is_truncated, nodes
            
        except Exception as e:
            self.logger.error(f"获取文件列表失败: {str(e)}", exc_info=True)
            raise

    def _get_directory_structure(self, path_or_node, parent_path="", depth=0, max_depth=10):
        """
        获取目录层级结构，用于前端展示
        返回格式符合前端预期的目录树结构
        
        :param path_or_node: 路径字符串或节点对象
        :param parent_path: 父目录路径，用于构建完整路径
        :param depth: 当前递归深度
        :param max_depth: 最大递归深度，防止栈溢出
        :return: 完整的树状目录结构
        """
        try:
            # 检查递归深度
            if depth > max_depth:
                self.logger.warning(f"递归深度超过限制({max_depth})，停止递归")
                return []
                
            # 执行认证
            if not self.auth.authenticate():
                raise Exception("爱数网盘认证失败")
            
            # 处理路径或节点参数
            folder_id = path_or_node
            current_parent_path = parent_path
            
            if isinstance(path_or_node, MutableMapping):
                folder_id = path_or_node.get('id', path_or_node.get('path', path_or_node.get('name', 'root')))
                current_parent_path = path_or_node.get('path', parent_path)
            
            self.logger.info(f"获取目录结构: {folder_id}, 父路径: {current_parent_path}, 深度: {depth}")
            
            # 调用API获取节点列表
            # 根据url的协议动态选择http或https
            protocol = "https" if self.url.startswith("https://") else "http"
            # 提取纯IP/域名，移除协议前缀
            base_host = self.url.replace("https://", "").replace("http://", "")
            api_url = f"{protocol}://{base_host}:9123/v1/entrydoc"
            
            params = {
                "method": "get",
                "userid": self.auth.userid,
                "tokenid": self.auth.tokenid,
                "folder_id": folder_id if folder_id and folder_id != 'root' else "root"
            }
            
            # 从节点中获取过滤参数（如果提供）
            if isinstance(path_or_node, MutableMapping):
                if 'start_time' in path_or_node:
                    params['start_time'] = path_or_node['start_time']
                    self.logger.debug(f"应用开始时间过滤: {params['start_time']}")
                if 'end_time' in path_or_node:
                    params['end_time'] = path_or_node['end_time']
                    self.logger.debug(f"应用结束时间过滤: {params['end_time']}")
            
            resp = self.session.get(api_url, params=params, timeout=300)
            
            if resp.status_code >= 300:
                self.logger.error(f"获取目录结构失败: {resp.status_code} - {resp.text}")
                raise Exception(f"获取节点列表失败: {resp.status_code}")
            
            data = resp.json()
            docinfos = data.get("docinfos", [])
            
            # 构建符合前端预期的目录结构
            result = []
            for doc in docinfos:
                # 爱数5.0 API返回的字段映射
                doc_type = doc.get('doctype')
                doc_name = doc.get('docname', 'unknown')
                
                # 构建节点基本信息
                docid = doc.get('docid', str(time.time()))
                
                # 构建完整路径
                if current_parent_path == "" or current_parent_path == "/":
                    full_path = f"/{doc_name}"
                else:
                    full_path = f"{current_parent_path}/{doc_name}"
                
                node = {
                    'id': docid,
                    'file_id': docid,
                    'docid': docid,
                    'name': doc_name,
                    'path': full_path,  # 使用完整路径
                    'size': doc.get('size', 0),
                    'type': 'file' if doc_type != 'dir' else 'dir',
                    'last_write_time': doc.get('modified_time', int(time.time())),
                    'create_time': doc.get('create_time', int(time.time())),
                    'update_time': doc.get('modified_time', int(time.time())),
                    'folder_id': folder_id if folder_id != 'root' else 'root',
                    'version': '1',
                    'children': [] if doc_type == 'dir' else None
                }
                
                # 对于目录，递归获取其子目录结构
                if node['type'] == 'dir':
                    try:
                        # 递归获取子目录结构，传递当前路径作为父路径
                        children = self._get_directory_structure(doc.get('docid'), full_path, depth + 1, max_depth)
                        node['children'] = children
                        self.logger.debug(f"成功获取子目录结构: {node['name']}, 子节点数量: {len(children)}")
                    except Exception as e:
                        self.logger.warning(f"获取子目录结构失败: {node['name']} - {str(e)}")
                        node['children'] = []  # 如果获取失败，设置为空数组
                
                result.append(node)
            
            self.logger.info(f"成功获取目录结构，返回{len(result)}个节点, 父路径: {current_parent_path}")
            return result
            
        except Exception as e:
            self.logger.error(f"获取目录结构异常: {str(e)}", exc_info=True)
            raise Exception(f"获取节点列表失败: {str(e)}")
    
    def get_file(self, node):
        """
        下载文件内容
        基于5.0版本API：{protocol}://{ip}:9123/v1/file?method=osdownload&userid=...&tokenid=...
        严格遵循现有get_file方法的返回规范
        """
        download_session = None
        try:
            # 确保认证仍然有效
            if not self.auth.userid or not self.auth.tokenid:
                if not self.auth.authenticate():
                    raise AuthenticationException("爱数网盘认证失败")

            # 爱数5.0版本文件下载API - 使用正确端口和端点
            # 根据url的协议动态选择http或https
            protocol = "https" if self.url.startswith("https://") else "http"
            # 提取纯IP/域名，移除协议前缀
            base_host = self.url.replace("https://", "").replace("http://", "")
            download_url = f"{protocol}://{base_host}:9123/v1/file"
            
            # 构造下载请求参数
            params = {
                "method": "osdownload",
                "userid": self.auth.userid,
                "tokenid": self.auth.tokenid
            }
            
            # 构造请求体 - 使用正确的docid字段
            docid = node.get('docid', node.get('file_id', node.get('gns_path', '')))
            if not docid:
                raise ValueError(f"文件缺少必要的标识字段: {node}")
                
            request_body = {
                "docid": docid
            }
            
            self.logger.info(f"下载文件: {node.get('name')}, docid: {request_body['docid']}")
            
            # 发送POST请求获取下载URL和认证信息
            resp = self.session.post(
                download_url,
                params=params,
                json=request_body,
                timeout=300
            )
            
            resp.raise_for_status()  # 自动处理HTTP错误
            
            data = resp.json()
            
            # 解析下载响应，获取预签名URL
            authrequest = data.get("authrequest", [])
            if not isinstance(authrequest, list) or len(authrequest) < 5:
                raise ValueError(f"下载响应格式错误: {data}")
            
            # 提取下载所需信息
            download_method = authrequest[0]  # GET/POST等
            download_url_from_response = authrequest[1]  # 实际下载URL
            
            # 使用更鲁棒的方式提取头信息
            def extract_header_value(header_str):
                """从"Header-Name: value"格式的字符串中提取值"""
                if ': ' in header_str:
                    return header_str.split(': ', 1)[1]
                return header_str
            
            date_header = extract_header_value(authrequest[2])
            length_header = extract_header_value(authrequest[3])
            auth_header = extract_header_value(authrequest[4])
            
            self.logger.debug(f"下载信息: method={download_method}, url={download_url_from_response[:50]}..., date={date_header}")
            
            # 使用获取的信息进行实际下载
            headers = {
                "X-Eoss-Date": date_header,
                "X-Eoss-Length": length_header,
                "Authorization": auth_header
            }
            
            # 创建新的session用于实际下载，避免影响主session的配置
            download_session = requests.Session()
            download_session.verify = self.session.verify  # 使用与主session相同的SSL验证设置
            # 设置请求重试策略
            retries = Retry(total=3, backoff_factor=0.5)
            adapter = HTTPAdapter(max_retries=retries)
            download_session.mount('http://', adapter)
            download_session.mount('https://', adapter)
            
            download_resp = download_session.get(
                download_url_from_response,
                headers=headers,
                timeout=300,
                stream=True
            )
            
            download_resp.raise_for_status()  # 自动处理HTTP错误
            
            # 读取文件内容
            file_stream = io.BytesIO()
            for chunk in download_resp.iter_content(chunk_size=8192):
                if chunk:
                    file_stream.write(chunk)
            
            file_stream.seek(0)
            file_content = file_stream.getvalue()
            
            self.logger.info(f"成功下载文件: {node.get('name')}, 大小: {len(file_content)} bytes")
            return file_content
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"文件下载网络异常: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"文件下载响应解析异常: {str(e)}")
            raise
        except ValueError as e:
            self.logger.error(f"文件下载参数异常: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"文件下载失败: {str(e)}")
            raise
        finally:
            # 确保资源清理
            if download_session:
                download_session.close()
    
    def test_connection(self):
        """
        测试连接
        基于5.0版本的测试方式
        """
        try:
            self.logger.info("开始连接测试...")
            # 尝试进行认证
            if self.auth.authenticate():
                self.logger.info("认证测试成功")
                # 尝试获取第一页数据（限制为1条）
                self.logger.debug("尝试获取根目录数据...")
                is_truncated, nodes = self.get_nodes_full(
                    page=1,
                    limit=1,
                    folder_id='root',
                    start_time=None,
                    end_time=None,
                    file_type=None
                )
                self.logger.info(f"连接测试成功，获取到 {len(nodes)} 个节点")
                return True
            self.logger.error("认证测试失败")
            return False
        except Exception as e:
            self.logger.error(f"连接测试失败: {str(e)}", exc_info=True)
            return False
    
    def __del__(self):
        """资源清理"""
        if hasattr(self, 'session'):
            self.session.close()
        if hasattr(self, 'auth') and hasattr(self.auth, 'session'):
            self.auth.session.close()
    
    def __bool__(self):
        """对象有效性判断"""
        return True


class AiShuIterator(Iterator):
    """
    爱数网盘迭代器
    基于5.0版本API，严格遵循现有Iterator基类的设计规范
    """
    # 构造函数
    def __init__(self, auth, resources={}):
        self.logger = logging.getLogger(self.__class__.__name__)
        # 记录接收到的所有参数
        self.logger.info(f"AiShuIterator初始化，接收到auth参数：{self._mask_sensitive_info(auth)}")
        self.logger.info(f"AiShuIterator初始化，接收到resources参数：{resources}")
        
        self.auth = auth
        self.client = AiShu(**self.auth)
        self.resources = resources
        self.page = 1
        self.is_truncated = True
        self.stack = list()
        self.node = dict()
        # 初始化基础参数，从resources中提取过滤条件
        self.folder_ids = resources.get('folder_id', [])
        if not isinstance(self.folder_ids, list):
            self.folder_ids = [self.folder_ids]
        if not self.folder_ids:
            self.folder_ids = ['root']
        
        # 将所有文件夹ID添加到栈中，支持处理多个文件夹
        for folder_id in self.folder_ids:
            folder_node = {
                'id': folder_id,
                'name': folder_id if folder_id != 'root' else '根目录',
                'path': '/' if folder_id == 'root' else f'/{folder_id}',
                'type': 'directory',
                'children': None
            }
            self.stack.append(folder_node)
        self.start_time = resources.get('start_time', None)
        self.end_time = resources.get('end_time', None)
        self.file_type = resources.get('file_type', None)
        self.page_size = resources.get('page_size', 100)
        
        self.logger.info("初始化爱数网盘5.0迭代器")
        
        # 初始化MinIO存储 - 方案A：沿用现有架构
        if STORAGE_AVAILABLE:
            # 从resources中提取存储参数
            identity = resources.get('identity', 'aishu_scan')
            rules_id = resources.get('rules_id', 'aishu_rules_default')
            task_id = resources.get('task_id', 'aishu_task_default')
            
            try:
                self.storage = MinioStorage(
                    identity=identity,
                    rules_id=rules_id,
                    task_id=task_id,
                    partitions=[],
                    url_enabled="0"
                )
                self.producer = Producer(
                    partitions=[],
                    client_id=identity,
                    **self.storage.variables.kafka['producer']
                )
                self.storage_enabled = True
                self.logger.info("MinIO存储和Kafka消息通知已启用")
            except Exception as e:
                self.storage = None
                self.producer = None
                self.storage_enabled = False
                self.logger.warning(f"存储组件初始化失败，将跳过文件存储和消息通知: {e}")
        else:
            self.storage = None
            self.producer = None
            self.storage_enabled = False
            self.logger.warning(f"存储组件不可用，将跳过文件存储和消息通知: {STORAGE_IMPORT_ERROR}")
        
        self.logger.info(f"初始化爱数网盘5.0迭代器，文件夹: {self.folder_ids}")
    
    def __iter__(self):
        return self

    def get_nodes(self, node):
        """
        获取节点的子节点
        与其他服务保持一致的接口
        """
        self.logger.info(f"调用get_nodes获取子节点，节点: {json.dumps(node, ensure_ascii=False)[:100]}...")
        return self.client.get_nodes(node)
    
    def __bool__(self):
        """
        判断迭代器是否还有元素
        与其他服务保持一致的接口
        """
        return bool(self.stack)
    
    def add_nodes(self, nodes):
        """
        添加节点到栈中
        与其他服务保持一致的接口
        """
        self.stack.extend(nodes)
        self.logger.debug(f"添加节点到栈中，栈大小: {len(self.stack)}")
    
    def __next__(self):
        """
        迭代逻辑 - 支持多个目录或文件的处理
        返回: (node, get_file_func)
        与其他服务（SMB、FTP）保持一致的迭代逻辑
        """
        while len(self.stack) > 0:
            # 从栈中取出一个节点
            self.node = AiShuDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'AISHU'
            
            self.logger.debug(f"处理节点: {self.node.get('name')} (类型: {self.node.get('type')}, ID: {self.node.get('file_id')})")
            
            if 'children' in self.node and self.node.get('type') == 'dir':
                # 只有目录节点才需要获取子节点
                if self.node['children']:
                    # 如果已有子节点，直接使用
                    children = self.node['children']
                else:
                    # 否则调用get_nodes获取子节点
                    children = self.get_nodes(self.node)
                
                # 将子节点逆序添加到栈中，确保处理顺序正确
                children.reverse()
                self.stack.extend(children)
                self.logger.debug(f"添加子节点到栈中，栈大小: {len(self.stack)}")
            else:
                # 如果是文件节点，返回节点和获取文件的函数
                return self.node, partial(
                    self.get_file,
                    self.node
                )
        
        # 所有资源处理完毕
        self.logger.info("所有资源处理完毕，迭代结束")
        raise StopIteration
    
    def get_file(self, node):
        """
        获取文件内容并计算MD5，自动存储到MinIO并发送Kafka消息通知
        严格遵循现有模式的实现，集成方案A的存储和消息机制
        """
        try:
            file_name = node.get('name', '未知文件名')
            file_id = node.get('file_id', '')
            self.logger.info(f"开始处理文件: {file_name} (ID: {file_id})")
            
            # 1. 下载文件内容
            self.logger.debug(f"调用client.get_file()下载文件: {file_name}")
            file_content = self.client.get_file(node)
            file_size = len(file_content)
            self.logger.info(f"文件下载成功: {file_name}, 大小: {file_size} 字节")
            
            # 2. 计算MD5
            self.logger.debug(f"计算文件MD5: {file_name}")
            md5 = hashlib.md5()
            md5.update(file_content)
            file_md5 = md5.hexdigest()
            node['md5'] = file_md5
            self.logger.info(f"MD5计算完成: {file_md5}")
            
            # 3. 准备文件信息用于存储和消息通知
            file_info = {
                'name': node.get('name', ''),
                'file_id': node.get('file_id', ''),
                'path': node.get('path', ''),
                'size': file_size,  # 使用实际文件大小
                'type': node.get('type', 'file'),
                'create_time': node.get('create_time', ''),
                'update_time': node.get('update_time', ''),
                'gns_path': node.get('gns_path', ''),
                'md5': file_md5
            }
            
            self.logger.debug(f"文件信息: {json.dumps(file_info, ensure_ascii=False, default=str)[:200]}...")
            
            # 4. 如果存储功能可用，执行存储和消息通知
            if self.storage_enabled:
                self.logger.debug("存储功能已启用")
                
                if self.storage:
                    try:
                        # 确定存储数据类型
                        data_type = 'text'  # 默认文本类型
                        file_ext = file_info['name'].split('.')[-1].lower() if '.' in file_info['name'] else ''
                        self.logger.debug(f"文件扩展名: {file_ext}, 原始大小: {file_size}")
                        
                        if file_ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff']:
                            data_type = 'file'
                        elif file_ext in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'zip', 'rar', '7z', 'exe', 'dmg']:
                            data_type = 'binary'
                        elif file_info['size'] > 1024 * 1024:  # 大于1MB
                            data_type = 'binary'
                        
                        self.logger.debug(f"确定存储类型: {data_type}")
                        
                        # 构造存储节点信息，遵循MinioStorage的接口要求
                        storage_node = {
                            'type': data_type,
                            'name': file_info['name'],
                            'path': file_info['path'],
                            'size': file_info['size'],
                            'create_time': file_info['create_time'],
                            'update_time': file_info['update_time'],
                            'md5': file_info['md5'],
                            'data': {}
                        }
                        
                        # 使用MinioStorage的__call__方法进行存储
                        self.logger.debug(f"调用self.storage()存储文件: {file_name}")
                        self.storage(storage_node, file_content)
                        
                        self.logger.info(f"文件存储成功: {file_info['name']}, 存储类型: {data_type}")
                        
                    except Exception as storage_error:
                        # 存储失败，发送错误消息但不中断处理
                        self.logger.error(f"文件存储失败: {str(storage_error)}", exc_info=True)
                        
                        # 只有在producer可用时才发送错误消息
                        if self.producer:
                            try:
                                error_message = {
                                    'event_type': 'file_storage_error',
                                    'storage': 'minio',
                                    'file_info': file_info,
                                    'error': str(storage_error),
                                    'timestamp': int(time.time() * 1000),
                                    'source': 'aishu_crawler'
                                }
                                self.logger.debug(f"发送存储错误消息: {json.dumps(error_message, ensure_ascii=False)[:200]}...")
                                self.producer.send_text(error_message, 'text_topic')
                                self.logger.info("存储错误消息发送成功")
                            except Exception as msg_error:
                                self.logger.warning(f"发送存储错误消息失败: {str(msg_error)}", exc_info=True)
                else:
                    self.logger.warning("存储功能已启用，但存储实例未初始化")
            else:
                self.logger.debug("存储功能未启用，跳过存储步骤")
            
            return file_content
            
        except Exception as e:
            self.logger.error(f"获取文件失败: {str(e)}", exc_info=True)
            
            # 发送处理错误消息（如果存储功能和producer可用）
            if self.storage_enabled and self.producer:
                try:
                    error_node_info = {
                        'name': node.get('name', 'unknown'),
                        'file_id': node.get('file_id', ''),
                        'path': node.get('path', '')
                    }
                    
                    error_message = {
                        'event_type': 'file_processing_error',
                        'storage': 'minio',
                        'file_info': error_node_info,
                        'error': str(e),
                        'timestamp': int(time.time() * 1000),
                        'source': 'aishu_crawler'
                    }
                    self.logger.debug(f"发送处理错误消息: {json.dumps(error_message, ensure_ascii=False)[:200]}...")
                    self.producer.send_text(error_message, 'text_topic')
                    self.logger.info("处理错误消息发送成功")
                except Exception as msg_error:
                    self.logger.warning(f"发送处理错误消息失败: {str(msg_error)}", exc_info=True)
            
            raise