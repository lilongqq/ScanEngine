#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from queue import SimpleQueue, Empty
from threading import Semaphore
from concurrent.futures import ThreadPoolExecutor


class BoundaryQueue(SimpleQueue):

    def __init__(self, maxsize=10):
        super().__init__()
        self.maxsize = Semaphore(maxsize)

    def put(self, item, block=True, timeout=None):
        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        if not self.maxsize.acquire(block, timeout):
            raise Empty
        return super().put(item)

    def get(self, block=True, timeout=None):
        item = super().get(block=block, timeout=timeout)
        self.maxsize.release()
        return item


class BoundaryThreadPoolExecutor(ThreadPoolExecutor):

    def __init__(
            self,
            max_workers=None,
            max_queue=None,
            thread_name_prefix='',
            initializer=None,
            initargs=()
    ):
        super().__init__(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
            initializer=initializer,
            initargs=initargs
        )
        if not max_queue:
            max_queue = max_workers * 2
        self._work_queue = BoundaryQueue(maxsize=max_queue)
