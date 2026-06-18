#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import traceback
from email.parser import BytesParser
from email.policy import SMTP
from common.functools import singleton


@singleton
class Parser(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parse = BytesParser(policy=SMTP)

    def __call__(self, node, data):
        if node.get('type', 'file') == 'file':
            if 'name' in node:
                name = node['name'].lower()
                if name.endswith('.eml'):
                    try:
                        headers = self.parse.parsebytes(data, headersonly=True)
                        for k, v in headers.items():
                            node[k.lower()] = v
                    except:
                        self.logger.error(traceback.format_exc())
