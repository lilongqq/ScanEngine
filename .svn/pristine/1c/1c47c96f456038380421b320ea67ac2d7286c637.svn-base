#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import json
import os
import os.path
import logging
import logging.config
import traceback
from functools import partial
from common.functools import get_class
from common.globals import variables
from exstd.concurrent.futures import BoundaryThreadPoolExecutor


class Downloads(object):

    def __init__(self):
        self.logger_config()
        self.logger = logging.getLogger(self.__class__.__name__)
        abspath = os.path.abspath(sys.argv[0])
        self.root = abspath[:abspath.find('/ScanEngine')]
        self.definition = self.load()
        self.iterator = get_class(self.definition['cls'])(
            **self.definition['iterator']
        )
        self.pool = BoundaryThreadPoolExecutor(**variables.executor)

    def logger_config(self):
        logging.config.dictConfig(variables.logging)

    def load(self):
        with open(os.path.join(self.root, 'downloads/download.json'), 'r') as f:
            definition = json.load(f)
        return definition

    def run(self):
        while True:
            try:
                node, stream = next(self.iterator)
                args = node, stream
                f = self.pool.submit(self.target, *args)
                f.add_done_callback(partial(self.logging_error, node))
            except StopIteration:
                self.pool.shutdown()
                self.logger.info('downloads is complete')
                break
            except Exception:
                self.logger.error(traceback.format_exc())

    def logging_error(self, node, future):
        exception = future.exception()
        if exception:
            self.logger.error(node)
            self.logger.error(traceback.format_exc())

    def target(self, node, stream):
        data = stream()
        path = os.path.join(self.root, 'downloads', node['path'])
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(path)
        with open(path, 'wb') as f:
            f.write(data)

if __name__ == "__main__":
    ins = Downloads()
    ins.run()
