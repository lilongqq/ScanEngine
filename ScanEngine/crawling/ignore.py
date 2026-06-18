#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from mq import Producer
from common.globals import Redis, Variables


class Ignore(object):

    def __init__(
            self,
            identity,
            rules_id,
            task_id
    ):
        self.identity = identity
        self.rules_id = rules_id
        self.task_id = task_id
        self.host_id = os.environ.get('HOST_ID')
        self.variables = Variables()
        self.client = Redis(**self.variables.redis['auth'])
        self.producer = Producer(
            **self.variables.kafka['producer']
        )
        self.ignore = ':'.join([self.variables.redis['ignore'], self.identity])

    def __call__(self, node):
        if 'md5' in node:
            ignore = self.client.hexists(self.ignore, node['md5'])
            if ignore:
                return False
            else:
                return True
        else:
            return True
