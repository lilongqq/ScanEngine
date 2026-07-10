#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import traceback
import os
import logging
import uuid
from threading import Lock
from common.functools import get_class
from common.globals import Variables
from mq import Producer
from concurrent.futures import ThreadPoolExecutor
from .discovery import Discovery


class StrategyFactory(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.producer = Producer(
            **self.variables.kafka['producer']
        )
        self.cache = dict()
        self.cache_lock = Lock()
        self.pool = ThreadPoolExecutor(max_workers=1)

    def dump(self, definition):
        identity = definition['identity']
        strategy_dir = os.path.join(self.variables.strategy, identity)
        if not os.path.exists(strategy_dir):
            os.makedirs(strategy_dir)
        with open(os.path.join(strategy_dir, 'strategy.json'), 'w') as f:
            json.dump(definition, f, ensure_ascii=False, indent=4)

    def load(self, identity):
        strategy_dir = os.path.join(self.variables.strategy, identity)
        with open(os.path.join(strategy_dir, 'strategy.json'), 'r') as f:
            definition = json.load(f)
        return definition

    def create(self, definition):
        self.dump(definition)

    def slave_start(self, identity):
        self.pool.submit(self.start, identity)

    def start(self, identity):
        definition = self.load(identity)
        with open(os.path.join(self.variables.strategy, 'strategy.json'), 'w') as f:
            json.dump(definition, f, ensure_ascii=False, indent=4)
        cls = definition['cls']
        partitions = definition['partitions']
        strategy = definition['strategy']
        rule_list = strategy.pop('ruleList', [])
        try:
            instance = get_class(cls)(identity, partitions, rule_list=rule_list, **strategy)
            with self.cache_lock:
                self.cache[identity] = instance
        except Exception:
            self.logger.error(traceback.format_exc())
            self.producer.send(
                blocking=True,
                topic=self.variables.kafka['status'],
                value={
                    'jobId': identity,
                    'taskId': uuid.uuid4().hex,
                    'initStatus': False
                }
            )
            raise
        try:
            instance.start()
        except Exception:
            self.logger.error(traceback.format_exc())
        finally:
            with self.cache_lock:
                self.cache.pop(identity, None)

    def slave_secondary(self, definition):
        self.pool.submit(self.secondary_start, definition)

    def secondary_start(self, definition):
        identity = definition['identity']
        cls = definition['cls']
        partitions = definition['partitions']
        strategy = definition['strategy']
        rule_list = strategy.pop('ruleList', [])
        try:
            instance = get_class(cls)(identity, partitions, rule_list=rule_list, **strategy)
            with self.cache_lock:
                self.cache[identity] = instance
        except Exception:
            self.logger.error(traceback.format_exc())
            self.producer.send(
                blocking=True,
                topic=self.variables.kafka['status'],
                value={
                    'jobId': identity,
                    'taskId': uuid.uuid4().hex,
                    'initStatus': False
                }
            )
            raise
        try:
            instance.start()
        except Exception:
            self.logger.error(traceback.format_exc())
        finally:
            with self.cache_lock:
                self.cache.pop(identity, None)

    def stop(self, identity):
        with self.cache_lock:
            instance = self.cache.get(identity)
        if instance:
            instance.stop()

    def slave_resume(self):
        self.pool.submit(self.resume)

    def resume(self):
        with open(os.path.join(self.variables.strategy, 'strategy.json'), 'r') as f:
            definition = json.load(f)
            identity = definition['identity']
            cls = definition['cls']
            partitions = definition['partitions']
            strategy = definition['strategy']
            rule_list = strategy.pop('ruleList', [])
        try:
            instance = get_class(cls)(identity, partitions, rule_list=rule_list, **strategy)
            with self.cache_lock:
                self.cache[identity] = instance
        except Exception:
            self.logger.error(traceback.format_exc())
            self.producer.send(
                blocking=True,
                topic=self.variables.kafka['status'],
                value={
                    'jobId': identity,
                    'taskId': uuid.uuid4().hex,
                    'initStatus': False
                }
            )
            raise
        try:
            instance.start()
        except Exception:
            self.logger.error(traceback.format_exc())
        finally:
            with self.cache_lock:
                self.cache.pop(identity, None)
