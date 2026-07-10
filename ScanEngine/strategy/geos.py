#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import time
import uuid
import traceback
from threading import Event
from exstd.threading import FalseCountSemaphore
from functools import partial
from common.redsync import RedEvent
from crawling.geos import classmap
from common.functools import get_class
from crawling.filters import FilterChain
from crawling.speeds import SpeedChain, ZeroChain
from crawling.ignore import Ignore
from crawling.diff import Differator
from crawling.geos.extractcrawler import ExtractIterator
from .interface import Strategy
from common.globals import Variables, Redis
from exceptions import FilterError
from dataprocessing import ProcessManager
from exstd.concurrent.futures import BoundaryThreadPoolExecutor
from mq import Producer


class Geospatial(Strategy):
    def __init__(
        self,
        identity,
        rules_id,
        cls,
        iterator,
        filters,
        action,
        speeds=[]
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.identity = identity
        self.rules_id = rules_id
        self.task_id = uuid.uuid4().hex
        self.create_time = int(time.time() * 1000)
        self.bool = True
        self.count = 0
        self.size = 0
        self.db_records = 0
        self.stats_name = ':'.join(
            [
                self.variables.redis['stats'],
                self.identity
            ]
        )
        self.redis = Redis(**self.variables.redis['auth'])
        self.iterator = get_class(classmap.iterator.get(cls))(**iterator)
        self.ignore = Ignore(
            self.identity,
            self.rules_id,
            self.task_id
            )
        self.differator = Differator(self.identity, self.rules_id)
        self.event = Event()
        self.semaphore = FalseCountSemaphore()
        self.pause_event = RedEvent(self.identity)
        self.zero_chain = ZeroChain(
            self.event,
            filter(lambda item: not item['speed'], speeds)
        )
        self.speed_chain = SpeedChain(
            self.event,
            filter(lambda item: item['speed'], speeds)
        )
        if cls in self.variables.differator['post']:
            self.pre_filter_chain = FilterChain({})
            self.post_filter_chain = FilterChain(filters)
            self.post_filter_chain.add(self.differator)
        else:
            self.pre_filter_chain = FilterChain(filters)
            self.post_filter_chain = FilterChain({})
            self.pre_filter_chain.add(self.differator)
        self.post_filter_chain.add(self.ignore)
        self.extractor = ExtractIterator()
        class_name = action.pop('cls')
        self.action = get_class(class_name)(
            self.identity,
            self.rules_id,
            self.task_id,
            **action
        )
        self.target = ProcessManager(
            target=self.action,
            speed_chain=self.speed_chain,
            pre_filter_chain=self.pre_filter_chain,
            post_filter_chain=self.post_filter_chain,
            extractor=self.extractor
        )
        self.pool = BoundaryThreadPoolExecutor(**self.variables.executor)
        self.producer = Producer(
            **self.variables.kafka['producer']
        )
        self.check_status()

    def check_status(self):
        status = self.redis.hget(self.stats_name, 'status')
        pipeline = self.redis.pipeline()
        if status in {None, b'stop'}:
            pipeline.hset(
                self.stats_name,
                'task_id',
                self.task_id
            )
            pipeline.hset(
                self.stats_name,
                'create_time',
                self.create_time
            )
            pipeline.hset(
                self.stats_name,
                'count',
                0
            )
            pipeline.hset(
                self.stats_name,
                'size',
                0
            )
            pipeline.hset(
                self.stats_name,
                'db_records',
                0
            )
            results = pipeline.execute()
            self.logger.info(results)
        else:
            self.task_id = str(
                self.redis.hget(self.stats_name, 'task_id'),
                encoding='utf-8'
            )
            self.create_time = int(
                self.redis.hget(
                    self.stats_name,
                    'create_time'
                )
            )
            self.count = int(
                self.redis.hget(
                    self.stats_name,
                    'count'
                )
            )
            self.size = int(
                self.redis.hget(
                    self.stats_name,
                    'size'
                )
            )
            self.db_records = int(
                self.redis.hget(
                    self.stats_name,
                    'db_records'
                )
            )

    def start(self):
        self.redis.hset(self.stats_name, 'status', 'start')
        self.report_task_start()
        self.run()
        self.report_task_completed()
        self.redis.hset(self.stats_name, 'status', 'stop')

    def run(self):
        while self.bool:
            try:
                self.pause_event.wait(180)
                node, stream = next(self.iterator)
                self.zero_chain()
                self.semaphore.acquire(blocking=False)
                try:
                    f = self.pool.submit(self.target, node, stream)
                    f.add_done_callback(partial(self.logging_error, node))
                except Exception:
                    self.semaphore.release()
                    raise
            except StopIteration:
                self.logger.info(self.identity)
                self.logger.info('StopIteration')
                break
            except Exception:
                self.logger.error(traceback.format_exc())
        self.semaphore.acquire()
        time.sleep(10)
        count, db_records, size = self.redis.hmget(
            self.stats_name,
            ['count', 'db_records', 'size']
        )
        if count:
            self.count = int(count)
        if db_records:
            self.db_records = int(db_records)
        if size:
            self.size = int(size)
        self.logger.info(
            '''
            identity: {identity}
            rules_id: {rules_id}
            task_id: {task_id}
            count: {count}
            db_records: {db_records}
            size: {size}
            '''.format(
                identity=self.identity,
                rules_id=self.rules_id,
                task_id=self.task_id,
                count=self.count,
                db_records=self.db_records,
                size=self.size
            )
        )
        self.logger.info('all task done')

    def logging_error(self, node, future):
        self.semaphore.release()
        exception = future.exception()
        pipeline = self.redis.pipeline()
        if exception:
            pipeline.hincrby(self.stats_name, 'count')
            if 'count' in node:
                pipeline.hincrby(
                    self.stats_name,
                    'db_records',
                    amount=node['count']
                )
            if 'size' in node:
                pipeline.hincrby(
                    self.stats_name,
                    'size',
                    amount=node['size']
                )
            node['jobId'] = [self.identity]
            node['taskId'] = self.task_id
            node['se_time'] = int(time.time()*1000)
            node['from'] = 'se'
            if isinstance(exception, FilterError):
                node['status'] = exception.status
                self.logger.info('%s  name: %s  path: %s', exception, node.data.get('name', ''), node.data.get('path', ''))
                self.producer.send(
                    topic=self.variables.kafka['filter'],
                    value=node.data
                )
            else:
                node['status'] = '540519'
                self.logger.error(traceback.format_exc())
                self.producer.send(
                    topic=self.variables.kafka['unprocessed'],
                    value=node.data
                )
            pipeline.execute()
            return
        result = future.result()
        if result:
            pipeline.hincrby(self.stats_name, 'count', amount=result)
            if 'count' in node:
                pipeline.hincrby(
                    self.stats_name,
                    'db_records',
                    amount=node['count']
                )
            if 'size' in node:
                pipeline.hincrby(
                    self.stats_name,
                    'size',
                    amount=node['size']
                )
            pipeline.execute()

    def report_task_start(self):
        self.producer.send(
            blocking=True,
            topic=self.variables.kafka['status'],
            value={
                'jobId': self.identity,
                'taskId': self.task_id,
                'createTime': self.create_time,
                'count': self.count,
                'size': self.size,
                'db_records': self.db_records,
                'rulesId': self.rules_id,
                'initStatus': True
            }
        )

    def report_task_completed(self):
        self.producer.send(
            blocking=True,
            topic=self.variables.kafka['status'],
            value={
                'jobId': self.identity,
                'taskId': self.task_id,
                'createTime': self.create_time,
                'completeTime': int(time.time() * 1000),
                'count': self.count,
                'db_records': self.db_records,
                'size': self.size,
                'rulesId': self.rules_id
            }
        )

    def stop(self):
        self.bool = False
        self.pause_event.set()
        self.event.set()

    def __bool__(self):
        return self.bool
