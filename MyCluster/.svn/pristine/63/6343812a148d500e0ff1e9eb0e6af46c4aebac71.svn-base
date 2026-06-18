#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from datetime import datetime
from crawling.emlparser import Parser


class ProcessManager(object):

    def __init__(self, target, speed_chain, filter_chain):
        self.target = target
        self.speed_chain = speed_chain
        self.filter_chain = filter_chain
        self.emlparser = Parser()

    def __call__(self, node, stream):
        curr_time = datetime.now()
        data = stream()
        self.speed_chain(node, curr_time)
        self.emlparser(node, data)
        if self.filter_chain(node):
            self.target(node, data)
        else:
            return False
        return True
