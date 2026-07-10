# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import os.path
import traceback
import logging
from common.globals import Redis, Variables
from common import classmap
from common.functools import get_class
from strategy import StrategyFactory
from smb.base import NotReadyError, NotConnectedError, SMBTimeout
from utils.database_found import database_found
from flask import Flask, request, send_file
from flask_httpauth import HTTPTokenAuth

auth = HTTPTokenAuth(scheme='Bearer')

tokens = {
    "umh7s4+bka%zt4#i6=1^ko": "i^ld*4o+(+fz_j624_)-#n8iaud"
}


@auth.verify_token
def verify_token(token):
    if token in tokens:
        return tokens[token]


class ScanEngine(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.app = Flask(self.__class__.__name__)
        self.variables = Variables()
        self.redis = Redis(**self.variables.redis['auth'])
        self.app.config["timeout"] = self.variables.flask['flask_response_timeout']

        self._load_api()
        self.strategy_factory = StrategyFactory()

    def _load_api(self):

        self.app.add_url_rule(
            '/opa',
            view_func=self.operate_job, methods=['POST'])
        self.app.add_url_rule(
            '/test',
            view_func=self.test_connect, methods=['POST'])
        self.app.add_url_rule(
            '/db_found',
            view_func=self.db_found, methods=['POST']
        )
        self.app.add_url_rule(
            '/db_info',
            view_func=self.db_info, methods=['POST']
        )
        self.app.add_url_rule(
            '/db_preview',
            view_func=self.db_preview, methods=['POST']
        )
        self.app.add_url_rule(
            '/download',
            view_func=self.download, methods=['POST']
        )
        self.app.add_url_rule(
            '/separate',
            view_func=self.separate, methods=['POST']
        )
        self.app.add_url_rule(
            '/restore',
            view_func=self.restore, methods=['POST']
        )
        self.app.add_url_rule(
            '/purge',
            view_func=self.purge, methods=['POST']
        )
        self.app.add_url_rule(
            '/count_size',
            view_func=self.count_size, methods=['POST']
        )

    def json_data(self, data, http_code=200):
        return data, http_code, {"Content-Type": "application/json"}

    @auth.login_required
    def db_preview(self):
        client = None
        try:
            req_body = request.json
            self.logger.info(req_body)
            node = req_body
            client = get_class(
                classmap.protocol.get(
                    node['cls']
                )
            )(
                **node['auth']
            )

            result = client.preview(
                node['path']
            ) if 'path' in node else client.preview(node['node'])

            return self.json_data({
                'code': 2000,
                'message': 'found ok',
                'data': result
            })
        except Exception:
            self.logger.error(traceback.format_exc())
            return self.json_data({'code': 5000, 'message': '服务器内部异常', 'data': []})
        finally:
            if client is not None and hasattr(client, '__del__'):
                try:
                    client.__del__()
                except Exception:
                    pass

    @auth.login_required
    def db_info(self):
        client = None
        try:
            req_body = request.json
            self.logger.info(req_body)
            node = req_body
            client = get_class(
                classmap.protocol.get(
                    node['cls']
                )
            )(
                **node['auth']
            )

            self.logger.info(client.db_info)

            return self.json_data({
                'code': 2000,
                'message': 'found ok',
                'data': client.db_info
            })
        except Exception:
            self.logger.error(traceback.format_exc())
            return self.json_data({'code': 5000, 'message': '服务器内部异常', 'data': []})
        finally:
            if client is not None and hasattr(client, '__del__'):
                try:
                    client.__del__()
                except Exception:
                    pass

    @auth.login_required
    def db_found(self):
        try:
            req_body = request.json
            self.logger.info(req_body)
            ip_range = req_body.get('ipRange')
            database_type = req_body.get('databaseType', 'Unlimited')
            ports = req_body.get('ports', [])
            self.logger.info(ip_range)
            self.logger.info(database_type)
            self.logger.info(ports)
            results = database_found(
                ip_range=ip_range,
                database_type=database_type,
                ports=ports
            )

            return self.json_data({
                'code': 2000,
                'message': '',
                'data': results
            })
        except Exception:
            self.logger.error(traceback.format_exc())
            return self.json_data({'code': 5000, 'message': '服务器内部异常', 'data': {}})

    @auth.login_required
    def test_connect(self):
        browser = None
        try:
            req_body = request.json
            self.logger.info(req_body)
            # TODO password
            node = req_body
            browser = get_class(classmap.protocol.get(
                node['cls']
            )
            )(
                **node['auth']
            )
            result = browser.get_nodes(
                node['path']
            ) if 'path' in node else browser.get_nodes(node['node'])
            self.logger.info(result)
            is_include_file = node.get('isIncludeFile', 1)
            if isinstance(result, list):
                result1 = [
                    node
                    for node in result
                    if 'children' in node
                ]  # dir
                if is_include_file:
                    result2 = [
                        node
                        for node in result
                        if 'children' not in node
                    ]  # file
                    result1.extend(result2)
                return self.json_data({
                    'code': 2000,
                    'message': '',
                    'data': result1
                })
            return self.json_data({
                'code': 2000,
                'message': '',
                'data': [r.data for r in result]
            })
        except TimeoutError:
            return self.json_data({'code': 4001, 'message': '网络连接异常', 'data': []})
        except (NotReadyError, NotConnectedError, SMBTimeout):
            return self.json_data({'code': 4002, 'message': '用户名或密码错误', 'data': []})
        except Exception:
            self.logger.error(traceback.format_exc())
            return self.json_data({'code': 5000, 'message': '服务器内部异常', 'data': []})
        finally:
            if browser is not None and hasattr(browser, '__del__'):
                try:
                    browser.__del__()
                except Exception:
                    pass

    @auth.login_required
    def download(self):
        client = None
        try:
            req_body = request.json
            self.logger.info(req_body)
            node = req_body
            client = get_class(
                classmap.protocol.get(
                    node['cls']
                )
            )(
                **node['auth']
            )
            f = client.get_file(node)
            return send_file(f, mimetype='application/octet-stream')
        except Exception:
            self.logger.error(traceback.format_exc())
            return self.json_data({'code': 5000, 'message': '服务器内部异常', 'data': []}, http_code=500)
        finally:
            if client is not None and hasattr(client, '__del__'):
                try:
                    client.__del__()
                except Exception:
                    pass

    @auth.login_required
    def separate(self):
        try:
            node = request.json
            self.logger.info(node)
            # 默认不打开占位符
            placeholder = node.get('placeholder', 0)
            try:
                src_client = get_class(
                    classmap.protocol.get(
                        node['cls']
                    )
                )(
                    **node['auth']
                )
            except Exception:
                self.logger.error(traceback.format_exc())
                return self.json_data({
                    'code': 4000,
                    'message': '服务器连接失败'
                })
            sep_path = node.get('sep_path')
            self.logger.info('sep_path:')
            self.logger.info(sep_path)

            # download file
            try:
                f = src_client.get_file(node)
            except Exception:
                self.logger.error(traceback.format_exc())
                return self.json_data({
                    'code': 4000,
                    'message': '获取源文件失败'
                })
            remote = node.get('remote', 1)
            if remote:
                try:
                    self.logger.info('sepCls:')
                    self.logger.info(node['sepCls'])
                    self.logger.info({k: '***' if 'pass' in k.lower() or 'key' in k.lower() else v
                                      for k, v in node['sepAuth'].items()})
                    target_client = get_class(
                        classmap.protocol.get(node['sepCls'])
                    )(
                        **node['sepAuth']
                    )
                    target_client.store_file(node, f)
                except Exception:
                    self.logger.error(traceback.format_exc())
                    return self.json_data({
                        'code': 4000,
                        'message': '远程隔离服务器连接失败'
                    })
            else:
                src_client.store_file(node, f)

            if placeholder:
                try:
                    src_client.empty_file(node)
                except Exception:
                    self.logger.error(traceback.format_exc())
            else:
                src_client.delete_file(node)
            return self.json_data({
                'code': 2000,
                'message': 'succeed'
            })
        except Exception:
            self.logger.error(traceback.format_exc())
            return self.json_data({'code': 5000, 'message': '服务器内部异常'})

    @auth.login_required
    def restore(self):
        try:
            node = request.json
            self.logger.info('before change node:')
            self.logger.info(node)
            # 交换节点信息
            node['auth'], node['sepAuth'] = node['sepAuth'], node['auth']
            node['cls'], node['sepCls'] = node['sepCls'], node['cls']
            node['path'], node['sepPath'] = node['sepPath'] + \
                '/' + node['name'], os.path.split(node['path'])[0]
            self.logger.info('after change node:')
            self.logger.info(node)
            try:
                src_client = get_class(
                    classmap.protocol.get(node['cls'])
                )(
                    **node['auth']
                )
            except Exception:
                self.logger.error(traceback.format_exc())
                return self.json_data({
                    'code': 4000,
                    'message': '服务器连接失败'
                })
            sep_path = node.get('sep_path')
            self.logger.info('sep_path:')
            self.logger.info(sep_path)

            # download file
            try:
                f = src_client.get_file(node)
            except Exception:
                self.logger.error(traceback.format_exc())
                return self.json_data(
                    {
                        'code': 4000,
                        'message': '获取源文件失败'
                    }
                )
            remote = node.get('remote', 1)
            if remote:
                try:
                    self.logger.info('sepCls:')
                    self.logger.info(node['sepCls'])
                    self.logger.info({k: '***' if 'pass' in k.lower() or 'key' in k.lower() else v
                                      for k, v in node['sepAuth'].items()})
                    target_client = get_class(
                        classmap.protocol.get(
                            node['sepCls']
                        )
                    )(
                        **node['sepAuth']
                    )
                    target_client.store_file(node, f)
                except Exception:
                    self.logger.error(traceback.format_exc())
                    return self.json_data({
                        'code': 4000,
                        'message': '远程隔离服务器连接失败'
                    })
            else:
                src_client.store_file(node, f)

            # 还原直接删除隔离区的文件
            src_client.delete_file(node)
            return self.json_data({
                'code': 2000,
                'message': 'succeed'
            })
        except Exception:
            self.logger.error(traceback.format_exc())
            return self.json_data({'code': 5000, 'message': '服务器内部异常'})

    @auth.login_required
    def purge(self):
        try:
            node = request.json
            self.logger.info(node)
            node['path'] = node['sepPath'] + '/' + node['name']
            remote = node.get('remote', 1)
            if remote:
                try:
                    _tgt_factory = get_class(classmap.protocol.get(node['sepCls']))
                    client = _tgt_factory(**node['sepAuth'])
                except Exception:
                    self.logger.error(traceback.format_exc())
                    return self.json_data({'code': 4000, 'message': '隔离服务器连接失败'})
            else:
                try:
                    _src_factory = get_class(classmap.protocol.get(node['cls']))
                    client = _src_factory(**node['auth'])
                except Exception:
                    self.logger.error(traceback.format_exc())
                    return self.json_data({'code': 4000, 'message': '服务器连接失败'})
            try:
                client.empty_file(node)
            except Exception:
                self.logger.error(traceback.format_exc())
            client.delete_file(node)
            return self.json_data({'code': 2000, 'message': 'succeed'})
        except Exception:
            self.logger.error(traceback.format_exc())
            return self.json_data({'code': 5000, 'message': '服务器内部异常'})

    @auth.login_required
    def operate_job(self):
        try:
            definition = request.json
            self.logger.info(definition)
            op = definition.get('op', 'create')
            identity = definition.get('identity')
            if op == 'manual_pause':
                self.redis.hset(
                    self.variables.redis['event'],
                    identity,
                    0
                )
                self.redis.publish(
                    self.variables.redis['event'],
                    identity
                )
            elif op == 'manual_resume':
                self.redis.hset(
                    self.variables.redis['event'],
                    identity,
                    1
                )
                self.redis.publish(
                    self.variables.redis['event'],
                    identity
                )
            elif op == 'create':
                func = getattr(self.strategy_factory, op)
                func(definition)
            else:
                func = getattr(self.strategy_factory, op)
                func(identity)
            return self.json_data({
                'code': 2000,
                'message': 'succeed'
            })
        except Exception:
            self.logger.error(traceback.format_exc())
            return self.json_data({'code': 5000, 'message': '服务器内部异常'})

    @auth.login_required
    def count_size(self):
        try:
            data = request.json
            self.logger.info(data)
            identity = data.get('identity', None)
            stats_name = ':'.join([self.variables.redis['stats'], identity])
            count, db_records, size, task_id, status = self.redis.hmget(
                stats_name,
                ['count', 'db_records', 'size', 'task_id', 'status']
            )
            if count:
                count = int(count)
            if db_records:
                db_records = int(db_records)
            if size:
                size = int(size)
            if task_id:
                task_id = str(task_id, encoding='utf-8')
            if status:
                status = str(status, encoding='utf-8')

            return self.json_data(
                {
                    'data': {
                        'count': count,
                        'size': size,
                        'db_records': db_records,
                        'task_id': task_id,
                        'status': status
                    },
                    'code': 2000,
                    'message': 'succeed'
                }
            )
        except Exception:
            self.logger.error(traceback.format_exc())
            return self.json_data({'code': 5000, 'message': '服务器内部异常'}, http_code=500)

    def run(self):
        self.app.run(
            host="0.0.0.0",
            port=8888,
            use_reloader=False,
            ssl_context=(
                os.path.join(self.variables.certs, 'server-cert.pem'),
                os.path.join(self.variables.certs, 'server-key.pem')
            )
        )
