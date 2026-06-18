#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模拟致远 OA REST 服务，用于在没有客户环境时测试 SeeyonIterator 下载流程。

支持的接口：
  POST /seeyon/rest/token/                          → 返回 mock token
  GET  /seeyon/rest/attachment/file/{ctp_file_id}  → 返回本地文件内容

文件存放规则：
  将文件放在 mock_files/ 目录下，文件名格式为 {ctp_file_id}.{任意扩展名}
  例如：mock_files/1001.docx、mock_files/1002.xlsx

启动方式：
  python mock_seeyon.py
  默认监听 0.0.0.0:8080
"""

import os
import glob
import logging
from flask import Flask, request, send_file, jsonify

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(name)-20s %(message)s'
)

app = Flask(__name__)
logger = logging.getLogger('MockSeeyon')

MOCK_TOKEN = 'mock-token-seeyon-debug-001'
FILES_DIR = os.path.join(os.path.dirname(__file__), 'mock_files')

os.makedirs(FILES_DIR, exist_ok=True)


@app.route('/seeyon/rest/token/', methods=['POST'])
def get_token():
    body = request.json or {}
    logger.info('[token] 收到请求: userName=%s, loginName=%s',
                body.get('userName'), body.get('loginName'))

    if not body.get('userName') or not body.get('loginName'):
        logger.warning('[token] 缺少 userName 或 loginName')
        return jsonify({'error': 'missing userName or loginName'}), 400

    resp = {
        'id':          MOCK_TOKEN,
        'bindingUser': body.get('loginName'),
        'userName':    body.get('userName'),
    }
    logger.info('[token] 返回 token: %s', MOCK_TOKEN)
    return jsonify(resp)


@app.route('/seeyon/rest/attachment/file/<ctp_file_id>', methods=['GET'])
def download_file(ctp_file_id):
    token = request.args.get('token', '')
    file_name = request.args.get('fileName', '')
    logger.info('[download] ctp_file_id=%s, fileName=%s, token=%s',
                ctp_file_id, file_name, token[:20] if token else 'None')

    if token != MOCK_TOKEN:
        logger.warning('[download] token 无效: %s', token)
        return jsonify({'error': 'Invalid token, please authenticate again'}), 401

    # 在 mock_files/ 里按 ctp_file_id 匹配文件（忽略扩展名）
    pattern = os.path.join(FILES_DIR, f'{ctp_file_id}.*')
    matches = glob.glob(pattern)
    if not matches:
        # 也尝试直接用 file_name 匹配
        if file_name:
            name_path = os.path.join(FILES_DIR, file_name)
            if os.path.isfile(name_path):
                matches = [name_path]

    if not matches:
        logger.warning('[download] 文件不存在: ctp_file_id=%s, 查找路径=%s', ctp_file_id, pattern)
        return jsonify({'error': f'file {ctp_file_id} not found'}), 404

    file_path = matches[0]
    logger.info('[download] 返回文件: %s', file_path)
    # Flask 1.x uses attachment_filename; Flask 2.x renamed it to download_name
    try:
        return send_file(file_path, as_attachment=True,
                         download_name=file_name or os.path.basename(file_path))
    except TypeError:
        return send_file(file_path, as_attachment=True,
                         attachment_filename=file_name or os.path.basename(file_path))


@app.route('/health', methods=['GET'])
def health():
    files = os.listdir(FILES_DIR)
    return jsonify({'status': 'ok', 'mock_files': files, 'token': MOCK_TOKEN})


if __name__ == '__main__':
    logger.info('Mock Seeyon OA 服务启动，监听 0.0.0.0:8080')
    logger.info('文件目录: %s', FILES_DIR)
    logger.info('可用文件: %s', os.listdir(FILES_DIR))
    app.run(host='0.0.0.0', port=8080, debug=False)
