#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import logging
import threading
from threading import Lock, Condition, Thread, Semaphore
from functools import partial
from datetime import timedelta
from datetime import datetime
from queue import SimpleQueue, Empty
from collections import deque
from collections.abc import Callable
from weakref import WeakValueDictionary
from exstd.json import default


class AliveCheck(object):

    def __init__(self, cls, *args, **kwargs):
        self.cls = cls
        self.args = args
        self.kwargs = kwargs
        self.client = self.cls(*self.args, **self.kwargs)
        self.time = datetime.now()

    def __getattr__(self, item):
        attr = getattr(self.client, item)
        if isinstance(attr, Callable):
            return partial(self.wrapper, item)
        else:
            return attr

    def wrapper(self, item, *args, **kwargs):
        try:
            self.time = datetime.now()
            if self.client:
                return getattr(self.client, item)(*args, **kwargs)
            else:
                old_client = self.client
                self.client = self.cls(*self.args, **self.kwargs)
                try:
                    del old_client
                except Exception:
                    pass
                return getattr(self.client, item)(*args, **kwargs)
        except Exception:
            raise
        finally:
            self.time = datetime.now()


class ClientPool(object):

    def __init__(self, max_connections, cls, *args,  **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.semaphore = Semaphore(max_connections)
        self.pool = SimpleQueue()
        self.cls = cls
        self.args = args
        self.kwargs = kwargs
        self.pool.put(
            self.cls(*self.args, **kwargs)
        )

    def __getattr__(self, item):
        return partial(self.wrapper, item)

    def wrapper(self, item, *args, **kwargs):
        try:
            self.semaphore.acquire()
            client = False
            try:
                client = self.pool.get_nowait()
            except Empty:
                client = self.cls(*self.args, **self.kwargs)
            if client:
                result = getattr(client, item)(*args, **kwargs)
                return result
            else:
                old_client = client
                client = self.cls(*self.args, **self.kwargs)
                try:
                    del old_client
                except Exception:
                    pass
                return getattr(client, item)(*args, **kwargs)
        except Exception:
            self.logger.error('ClientPool.wrapper exception: %s', traceback.format_exc())
            raise
        finally:
            self.pool.put(client)
            self.semaphore.release()

    def __del__(self):
        while True:
            try:
                self.pool.get_nowait()
            except Empty:
                break


class ThreadLocal(object):

    def __init__(self, cls, *args, **kwargs):
        self.cls = cls
        self.args = args
        self.kwargs = kwargs
        self.pool = threading.local()
        self.pool.client = self.cls(*self.args, **self.kwargs)

    def __getattr__(self, item):
        return partial(self.wrapper, item)

    def wrapper(self, item, *args, **kwargs):
        client = getattr(self.pool, 'client', None)
        if not client:
            if client is not None:
                try:
                    del client
                except Exception:
                    pass
            self.pool.client = self.cls(*self.args, **self.kwargs)
        return getattr(self.pool.client, item)(*args, **kwargs)


class WeakCache(object):
    def __init__(self, cls):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cls = cls
        self.lock = Lock()
        self.cache = WeakValueDictionary()

    def __call__(self, *args, **kwargs):
        parameters = dict()
        parameters['args'] = args
        parameters['kwargs'] = kwargs
        key = json.dumps(
            parameters,
            ensure_ascii=False,
            indent=4,
            default=default
        )
        try:
            self.lock.acquire()
            if key in self.cache:
                return self.cache[key]
            else:
                client = self.cls(*args, **kwargs)
                self.cache[key] = client
                return client
        except Exception:
            raise
        finally:
            self.lock.release()


class RequestLimit(object):

    def __init__(self, period, counts, cls, *args, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.period = timedelta(seconds=period)
        self.counts = counts
        self.cls = cls
        self.args = args
        self.kwargs = kwargs
        self.queue = deque()
        self.cond = Condition()
        self.client = self.cls(*self.args, **self.kwargs)

    def __getattr__(self, item):
        attr = getattr(self.client, item)
        if isinstance(attr, Callable):
            return partial(self.wrapper, item)
        else:
            return attr

    def wrapper(self, item, *args, **kwargs):
        try:
            self.cond.acquire()
            if self.counts <= 0:
                release_time = self.queue.popleft()
                now = datetime.now()
                if release_time >= now:
                    delta = release_time - now
                    self.logger.info('wait request time release')
                    self.cond.wait(delta.total_seconds())
                self.counts += 1
                self.logger.info('request time released')
            self.counts -= 1
            if self.client:
                return getattr(self.client, item)(*args, **kwargs)
            else:
                old_client = self.client
                self.client = self.cls(*self.args, **self.kwargs)
                try:
                    del old_client
                except Exception:
                    pass
                return getattr(self.client, item)(*args, **kwargs)
        except Exception:
            raise
        finally:
            now = datetime.now()
            self.queue.append(now + self.period)
            self.cond.release()


def alive_check(cls):
    """
    check alive, otherwise reset the client, before execute.
    """
    return partial(AliveCheck, cls)


def client_pool(max_connections):
    """
    pool clients for thread
    """
    def wrapper(cls):
        return partial(ClientPool, max_connections, cls)

    return wrapper


def thread_local(cls):
    """
    pool clients thread local
    """
    return partial(ThreadLocal, cls)


def weak_cache(cls):
    """
    weak cache
    """
    return WeakCache(cls)


def request_limit(period=1800, counts=600):
    """
    limit the request times in a period
    """

    def wrapper(cls):
        return partial(RequestLimit, period, counts, cls)

    return wrapper
