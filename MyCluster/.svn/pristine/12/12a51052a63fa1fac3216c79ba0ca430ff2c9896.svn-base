#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from threading import Event
from common.globals import Variables, Redis


class RedEvent(object):

    def __init__(self, identity):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.identity = identity
        self.variables = Variables()
        self.redis = Redis(**self.variables.redis['auth'])
        self.pubsub = self.redis.pubsub()
        self.event = Event()
        if self.is_set():
            self.event.set()
        else:
            self.event.clear()
        self.pubsub.subscribe(**{self.variables.redis['event']: self.callback})
        self.pubsub.run_in_thread(daemon=True)

    def is_set(self):
        pipeline = self.redis.pipeline()
        pipeline.hget(
            self.variables.redis['event'],
            'all'
        )
        pipeline.hget(
            self.variables.redis['event'],
            self.identity
        )
        all, identity = pipeline.execute()
        if all:
            all = int(all)
        else:
            all = 1
        if identity:
            identity = int(identity)
        else:
            identity = 1
        return all and identity

    def set(self):
        self.redis.hset(
            self.variables.redis['event'],
            self.identity,
            1
        )
        self.event.set()

    def clear(self):
        self.redis.hset(
            self.variables.redis['event'],
            self.identity,
            0
        )
        self.event.clear()

    def wait(self, timeout=60):
        while True:
            wait = self.event.wait(timeout=timeout)
            if wait:
                break
            else:
                if self.is_set():
                    self.event.set()

    def callback(self, message):
        if message['type'] == 'subscribe':
            self.logger.info(message)
        elif message['type'] == 'message':
            self.logger.info(message)
            data = message['data'].decode('utf-8')
            if data in ('all', self.identity):
                if self.is_set():
                    self.event.set()
                else:
                    self.event.clear()
