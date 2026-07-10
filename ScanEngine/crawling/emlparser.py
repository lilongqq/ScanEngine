#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import traceback
from io import BytesIO, StringIO, FileIO, TextIOWrapper
from tempfile import SpooledTemporaryFile
from functools import singledispatchmethod
from email import parser
from email.policy import HTTP
from email.feedparser import FeedParser
from common.functools import singleton


class BytesParser(parser.Parser):
    def __init__(self, *args, max_lines=1000, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_lines = max_lines

    @singledispatchmethod
    def parse(self, *args, **kwargs):
        raise NotImplemented

    @parse.register(bytes)
    @parse.register(bytearray)
    def _(self, text, headersonly=False):
        return self.parse(BytesIO(text), headersonly=headersonly)

    @parse.register(str)
    def _(self, text, headersonly=False):
        return self.parse(StringIO(text), headersonly=headersonly)

    @parse.register(BytesIO)
    @parse.register(FileIO)
    @parse.register(SpooledTemporaryFile)
    def _(self, fp, headersonly=False):
        raw = fp
        fp = TextIOWrapper(fp, encoding='ascii', errors='surrogateescape')
        try:
            return self.parse(fp, headersonly)
        finally:
            fp.detach()
            raw.seek(0)

    @parse.register(TextIOWrapper)
    @parse.register(StringIO)
    def _(self, fp, headersonly=False):
        feedparser = FeedParser(self._class, policy=self.policy)
        if headersonly:
            feedparser._set_headersonly()
        count = 0
        header = False
        while count < self.max_lines:
            data = fp.readline()
            count += 1
            if data:
                if data.isspace() and header:
                    break
                else:
                    header = True
                    feedparser.feed(data)
            else:
                break
        return feedparser.close()


@singleton
class Parser(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parser = BytesParser(policy=HTTP)

    def __call__(self, node, data):
        if node.get('type', 'file') == 'file':
            if 'name' in node:
                name = node['name'].lower()
                if name.endswith('.eml'):
                    try:
                        headers = self.parser.parse(data, headersonly=True)
                        for k, v in headers.items():
                            node[k.lower()] = v
                    except:
                        self.logger.error(traceback.format_exc())
