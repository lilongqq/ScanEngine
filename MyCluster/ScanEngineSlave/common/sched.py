#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess
import io
import traceback
import logging
import heapq
from functools import partial
from collections import UserDict
from threading import Thread, Condition
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from weakref import WeakValueDictionary


class SchedulerDict(UserDict):

    def __lt__(self, other):
        return self.data.get('date_time') < other['date_time']

    def __le__(self, other):
        return self.data.get('date_time') <= other['date_time']

    def __ge__(self, other):
        return self.data.get('date_time') >= other['date_time']

    def __gt__(self, other):
        return self.data.get('date_time') > other['date_time']


class Scheduler(Thread):

    def __init__(self):
        super().__init__(daemon=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pool = ThreadPoolExecutor()
        self.bool = True
        self.queue = list()
        self.cache = WeakValueDictionary()
        self.cond = Condition()

    @staticmethod
    def calendar_event(event):
        resp = subprocess.run(
            ["systemd-analyze", "calendar", event['on_calendar']],
            capture_output=True,
            encoding='utf-8'
        )
        if resp.returncode == 0:
            for line in io.StringIO(resp.stdout):
                k, v = line.split(':', 1)
                if k.strip() == 'Next elapse':
                    if v.strip() == 'never':
                        raise ValueError('never')
                    else:
                        event['date_time'] = datetime.strptime(v.strip(), '%a %Y-%m-%d %H:%M:%S %Z')
                        break
        else:
            raise ValueError(resp.stderr)

    @staticmethod
    def active_event(event):
        current_time = datetime.now()
        execute_time = event['date_time']
        while True:
            execute_time = execute_time + event['on_active']
            if execute_time > current_time:
                event['date_time'] = execute_time
                break

    @staticmethod
    def inactive_event(event):
        execute_time = datetime.now() + event['on_inactive']
        event['date_time'] = execute_time

    @classmethod
    def init_event(cls, event):
        if not event.get('date_time'):
            if event.get('on_calendar'):
                cls.calendar_event(event)
            else:
                event['date_time'] = datetime.now()

    def enterabs(
            self,
            identity,
            action,
            args=(),
            kwargs={},
            date_time=None,
            on_active=None,
            on_inactive=None,
            on_calendar=None,
            cancelled=False
    ):
        if cancelled:
            self.logger.info('cycle event is cancelled')
            self.logger.info('identity: {}'.format(identity))
        else:
            with self.cond:
                event = SchedulerDict()
                event['identity'] = identity
                event['action'] = action
                event['args'] = args
                event['kwargs'] = kwargs
                if date_time:
                    event['date_time'] = date_time
                if on_active:
                    event['on_active'] = on_active
                if on_inactive:
                    event['on_inactive'] = on_inactive
                if on_calendar:
                    event['on_calendar'] = on_calendar
                self.init_event(event)
                heapq.heappush(self.queue, event)
                self.cancel(identity)
                self.cache[identity] = event
                self.cond.notify()
                self.logger.info('event is in queue')
                self.logger.info('identity: {}'.format(identity))

    def cancel(self, identity):
        event = self.cache.get(identity)
        if event:
            event['cancelled'] = True

    def run(self) -> None:
        while self.bool:
            self.cond.acquire()
            try:
                if self.queue:
                    event = self.queue[0]
                    execute_time = event['date_time']
                    current_time = datetime.now()
                    if execute_time > current_time:
                        time_delta = execute_time - current_time
                        timeout = time_delta.total_seconds()
                        self.logger.info('wait {} seconds'.format(timeout))
                        if self.cond.wait(timeout=timeout):
                            self.logger.info('event is created')
                        else:
                            self.logger.info('wait time is enough')
                            event = heapq.heappop(self.queue)
                            self.schedule(event)
                    else:
                        self.logger.info('run right now')
                        event = heapq.heappop(self.queue)
                        self.schedule(event)
                else:
                    self.logger.info('no event in queue')
                    self.cond.wait()
                    self.logger.info('event is created')
            finally:
                self.cond.release()

    def schedule(self, event):
        if event.get('cancelled'):
            self.logger.info('event is cancelled')
            self.logger.info(
                'identity: {}'.format(event['identity'])
            )
        else:
            f = self.pool.submit(event['action'], *event['args'], **event['kwargs'])
            if 'on_active' in event:
                f.add_done_callback(partial(self.on_active, event))
            elif 'on_inactive' in event:
                f.add_done_callback(partial(self.on_inactive, event))
            elif 'on_calendar' in event:
                f.add_done_callback(partial(self.on_calendar, event))
            else:
                f.add_done_callback(self.onetime)

    def onetime(self, future):
        exception = future.exception()
        if exception:
            self.logger.error(''.join(traceback.format_exception(type(exception), exception, exception.__traceback__)))

    def on_active(self, event, future):
        exception = future.exception()
        if exception:
            self.logger.error(''.join(traceback.format_exception(type(exception), exception, exception.__traceback__)))
        self.active_event(event)
        self.enterabs(**event)

    def on_inactive(self, event, future):
        exception = future.exception()
        if exception:
            self.logger.error(''.join(traceback.format_exception(type(exception), exception, exception.__traceback__)))
        self.inactive_event(event)
        self.enterabs(**event)

    def on_calendar(self, event, future):
        exception = future.exception()
        if exception:
            self.logger.error(''.join(traceback.format_exception(type(exception), exception, exception.__traceback__)))
        try:
            self.calendar_event(event)
        except ValueError:
            self.logger.info(traceback.format_exc())
        else:
            self.enterabs(**event)
