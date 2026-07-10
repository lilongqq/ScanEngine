#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import traceback
import os
import shutil
import logging
import uuid
from threading import Timer, Lock
from common.functools import get_class
from common.globals import Variables
from mq import Producer
from common.sched import Scheduler
from datetime import datetime, timedelta
from .discovery import Discovery
from .shanxioa import ShanxiOA
from .geos import Geospatial


class StrategyFactory(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.producer = Producer(
            **self.variables.kafka['producer']
        )
        self.cache = dict()
        self.cache_lock = Lock()
        self.scheduler = Scheduler()
        self.scheduler.start()

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
        identity = definition['identity']
        scheduler = definition['scheduler']
        self.sch(identity, scheduler)

    def sch(self, identity, scheduler):
        event = dict()
        event['identity'] = identity
        event['action'] = self.operate
        if scheduler.get('date_time'):
            event['date_time'] = datetime.fromisoformat(scheduler['date_time'])
        if scheduler.get('on_active'):
            event['on_active'] = timedelta(**scheduler['on_active'])
        if scheduler.get('on_inactive'):
            event['on_inactive'] = timedelta(**scheduler['on_inactive'])
        if scheduler.get('on_calendar'):
            event['on_calendar'] = scheduler['on_calendar']
        args = (identity, scheduler['op'])
        kwargs = {}
        if scheduler.get('continuous'):
            kwargs['continuous'] = scheduler['continuous']
        event['args'] = args
        event['kwargs'] = kwargs
        self.scheduler.enterabs(**event)

    def manual_start(self, identity):
        if identity not in self.cache:
            self.scheduler.cancel(identity)
            definition = self.load(identity)
            scheduler = definition['scheduler']
            scheduler['date_time'] = datetime.now().isoformat()
            self.sch(identity, scheduler)

    def start(self, identity):
        definition = self.load(identity)
        cls = definition['cls']
        strategy = definition['strategy']
        rule_list = strategy.pop('ruleList', [])
        try:
            instance = get_class(cls)(identity, rule_list=rule_list, **strategy)
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
                del self.cache[identity]

    def secondary(self, definition):
        identity = definition['identity']
        event = dict()
        event['identity'] = identity
        event['action'] = self.secondary_start
        args = (definition,)
        kwargs = {}
        event['args'] = args
        event['kwargs'] = kwargs
        self.scheduler.enterabs(**event)

    def secondary_start(self, definition):
        identity = definition['identity']
        cls = definition['cls']
        strategy = definition['strategy']
        rule_list = strategy.pop('ruleList', [])
        try:
            instance = get_class(cls)(identity, rule_list=rule_list, **strategy)
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
                del self.cache[identity]

    def delete(self, identity):
        self.scheduler.cancel(identity)
        self.stop(identity)
        strategy_dir = os.path.join(self.variables.strategy, identity)
        if os.path.isdir(strategy_dir):
            shutil.rmtree(strategy_dir)

    def manual_stop(self, identity):
        self.scheduler.cancel(identity)
        self.stop(identity)

    def stop(self, identity):
        with self.cache_lock:
            instance = self.cache.get(identity)
        if instance:
            instance.stop()

    def operate(self, identity, op, continuous=None):
        if op == 'start':
            if continuous:
                t = Timer(
                    timedelta(**continuous).total_seconds(),
                    self.stop,
                    args=(identity,)
                )
                t.daemon = True
                t.start()
                self.start(identity)
                t.cancel()
            else:
                self.start(identity)
