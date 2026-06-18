#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import traceback
import io
from datetime import datetime
from crawling.emlparser import Parser


class ProcessManager(object):

    def __init__(
        self,
        target,
        speed_chain,
        pre_filter_chain,
        post_filter_chain,
        extractor=None
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.target = target
        self.speed_chain = speed_chain
        self.pre_filter_chain = pre_filter_chain
        self.post_filter_chain = post_filter_chain
        self.extractor = extractor
        self.emlparser = Parser()

    def __call__(self, node, stream):
        count = 0
        if self.pre_filter_chain(node):
            curr_time = datetime.now()
            data = stream()
        else:
            return count
        try:
            self.speed_chain(node, curr_time)
            self.emlparser(node, data)
            if self.post_filter_chain(node):
                if self.extractor:
                    try:
                        self.extractor(node, data)
                    except:
                        self.logger.error(traceback.format_exc())
                        if isinstance(data, io.IOBase) and data.seekable():
                            data.seek(0)
                        self.target(node, data)
                        count += 1
                    else:
                        for node, data in self.extractor:
                            self.target(node, data)
                            count += 1
                else:
                    self.target(node, data)
                    count += 1
        finally:
            if isinstance(data, io.IOBase):
                data.close()
        return count
