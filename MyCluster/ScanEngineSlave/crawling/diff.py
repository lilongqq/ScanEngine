#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from common.globals import Redis, Variables


class Differator(object):

    def __init__(self, strategy, rules):
        self.strategy = strategy
        self.rules = rules
        self.variables = Variables()
        self.client = Redis(**self.variables.redis['auth'])
        self.name = ':'.join(
            [
                self.variables.redis['diff'],
                self.strategy,
                self.rules
            ]
        )
        self.exists = self.client.exists(self.name)

    def __call__(self, node):
        if self.exists:
            if 'diff' in node:
                return not self.client.sismember(self.name, node['diff'])
            else:
                return True
        else:
            return True
