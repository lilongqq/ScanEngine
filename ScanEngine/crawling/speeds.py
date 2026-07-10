#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from datetime import datetime, time, timedelta, date
from functools import partial
from common.globals import Variables


class SpeedChain(object):

    def __init__(self, event, speeds):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.event = event
        self.variables = Variables()
        self.coeff = self.variables.executor['max_workers']
        self.chain = list()
        self.generate(speeds)

    def limit(
        self,
        node,
        start_time,
        begin,
        end,
        speed,
        isoweekdays=[1, 2, 3, 4, 5, 6, 7]
    ):
        curr_time = datetime.now()
        if curr_time.isoweekday() not in isoweekdays:
            self.logger.info(curr_time)
        elif time.fromisoformat(begin) <= curr_time.time() <= time.fromisoformat(end):
            estimate = (node['size'] * self.coeff) // (speed * 1024 * 1024)
            if estimate > (curr_time - start_time).total_seconds():
                end_time = datetime.combine(
                    date=date.today(),
                    time=time.fromisoformat(end)
                )
                waiting = (
                    start_time +
                    timedelta(seconds=estimate) -
                    curr_time
                ).total_seconds()
                bounding = (end_time - curr_time).total_seconds()
                if waiting > bounding:
                    waiting = bounding
                self.logger.info('block')
                self.event.wait(timeout=waiting)
                self.logger.info('open')

    def generate(self, args):
        for kwargs in args:
            self.chain.append(
                partial(self.limit, **kwargs)
            )

    def __call__(self, node, start_time):
        for fun in self.chain:
            fun(node, start_time)


class ZeroChain(object):

    def __init__(self, event, speeds):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.event = event
        self.chain = list()
        self.generate(speeds)

    def limit(
        self,
        begin,
        end,
        speed,
        isoweekdays=[1, 2, 3, 4, 5, 6, 7]
    ):
        curr_time = datetime.now()
        if curr_time.isoweekday() not in isoweekdays:
            self.logger.info(curr_time)
        elif time.fromisoformat(begin) <= curr_time.time() <= time.fromisoformat(end):
            end_time = datetime.combine(date=date.today(), time=time.fromisoformat(end))
            waiting = (end_time - curr_time).total_seconds()
            self.logger.info('speed is %s, block until %s (%.0fs)', speed, end_time.strftime('%H:%M:%S'), waiting)
            self.logger.info('block')
            self.event.wait(timeout=waiting)
            self.logger.info('open')

    def generate(self, args):
        for kwargs in args:
            self.chain.append(
                partial(self.limit, **kwargs)
            )

    def __call__(self):
        for fun in self.chain:
            fun()
