#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from common.globals import Redis, Variables


class Ignore(object):

    def __init__(self, identity):
        self.identity = identity
        self.variables = Variables()
        self.client = Redis(**self.variables.redis['auth'])
        self.name = ':'.join([self.variables.redis['ignore'], self.identity])

    def __call__(self, node):
        if 'md5' in node:
            return not self.client.hexists(self.name, node['md5'])
        else:
            return True
