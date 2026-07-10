#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import logging.config
import time
import uuid
import traceback
from crawling.filters import FilterChain
from common import classmap
from common.functools import get_class
from crawling.ignore import Ignore
from crawling.diff import Differator
from transfer import S3
from .interface import Strategy
from common.globals import Variables, Redis
from exceptions import FilterError
from mq import Producer


class Discovery(Strategy):
    def __init__(
        self,
        identity,
        partitions,
        rules_id,
        cls,
        iterator,
        filters,
        **kwargs
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.identity = identity
        self.task_id = uuid.uuid4().hex
        self.partitions = partitions
        self.rules_id = rules_id
        self.bool = True
        self.create_time = int(time.time() * 1000)
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
        self.pipeline = self.redis.pipeline()
        self.iterator = get_class(classmap.iterator.get(cls))(**iterator)
        self.ignore = Ignore(self.identity)
        self.differator = Differator(self.identity, self.rules_id)
        self.transfer = S3(**self.variables.minio)
        self.transfer.makedirs(self.identity)
        self.producer = Producer(
            partitions=self.partitions,
            client_id=identity,
            **self.variables.kafka['producer']
        )
        if cls in self.variables.differator['post']:
            self.filter_chain = FilterChain({})
        else:
            self.filter_chain = FilterChain(filters)
            self.filter_chain.add(self.ignore)
            self.filter_chain.add(self.differator)
        self.check_status()

    def check_status(self):
        status = self.redis.hget(self.stats_name, 'status')
        if status not in {None, b'stop'}:
            task_id_bytes = self.redis.hget(self.stats_name, 'task_id')
            if task_id_bytes:
                self.task_id = str(task_id_bytes, encoding='utf-8')
            create_time = self.redis.hget(self.stats_name, 'create_time')
            if create_time:
                self.create_time = int(create_time)
            self.pipeline.hset(
                self.stats_name,
                'subtasks',
                len(self.partitions)
            )
            results = self.pipeline.execute()
            self.logger.info(results)
        else:
            self.pipeline.hset(
                self.stats_name,
                'task_id',
                self.task_id
            )
            self.pipeline.hset(
                self.stats_name,
                'create_time',
                self.create_time
            )
            self.pipeline.hset(
                self.stats_name,
                'subtasks',
                len(self.partitions)
            )
            self.pipeline.hset(
                self.stats_name,
                'count',
                0
            )
            self.pipeline.hset(
                self.stats_name,
                'size',
                0
            )
            self.pipeline.hset(
                self.stats_name,
                'db_records',
                0
            )
            results = self.pipeline.execute()
            self.logger.info(results)

    def start(self):
        self.redis.hset(self.stats_name, 'status', 'start')
        self.report_task_start()
        self.run()
        self.logger.info(
            '''
            identity: {identity}
            rules_id: {rules_id}
            count: {count}
            db_records: {db_records}
            size: {size}
            '''.format(
                identity=self.identity,
                rules_id=self.rules_id,
                count=self.count,
                db_records=self.db_records,
                size=self.size
            )
        )

    def run(self):
        consecutive_errors = 0
        while self.bool:
            try:
                node = next(self.iterator)
                consecutive_errors = 0
                node['taskId'] = self.task_id
                self.logger.debug(node)
                if self.filter_chain(node):
                    self.producer.send(
                        topic=self.variables.kafka['attributes'],
                        value=node.data
                    )
                    self.count += 1
                    if 'size' in node:
                        self.size += node['size']
            except StopIteration:
                self.producer.flush()
                self.producer.send_all(
                    partitions=self.partitions,
                    topic=self.variables.kafka['attributes'],
                    value={
                        'identity': self.identity,
                        'type': 'stop_iteration'
                    }
                )
                self.logger.info('StopIteration')
                break
            except FilterError as exception:
                node['jobId'] = [self.identity]
                node['taskId'] = self.task_id
                node['rulesId'] = self.rules_id
                node['se_time'] = int(time.time()*1000)
                node['from'] = 'se'
                node['status'] = exception.status
                self.logger.debug(node)
                self.producer.send(
                    topic=self.variables.kafka['filter'],
                    value=node.data
                )
                if 'size' in node:
                    self.pipeline.hincrby(
                        self.stats_name,
                        'size',
                        amount=node['size']
                    )
                self.pipeline.hincrby(self.stats_name, 'count')
                self.pipeline.execute()
            except Exception:
                consecutive_errors += 1
                self.logger.info(traceback.format_exc())
                if consecutive_errors >= 50:
                    self.logger.error('too many consecutive errors (%d), stopping', consecutive_errors)
                    break
                elif consecutive_errors >= 5:
                    time.sleep(min(consecutive_errors * 0.5, 30))
                node = self.iterator.node
                node['jobId'] = [self.identity]
                node['taskId'] = self.task_id
                node['rulesId'] = self.rules_id
                node['se_time'] = int(time.time()*1000)
                node['from'] = 'se'
                node['status'] = '540519'
                self.logger.debug(node)
                self.producer.send(
                    topic=self.variables.kafka['unprocessed'],
                    value=node.data
                )
                if 'size' in node:
                    self.pipeline.hincrby(
                        self.stats_name,
                        'size',
                        amount=node['size']
                    )
                self.pipeline.hincrby(self.stats_name, 'count')
                self.pipeline.execute()
        else:
            self.producer.flush()
            self.producer.send_all(
                partitions=self.partitions,
                topic=self.variables.kafka['attributes'],
                value={
                    'identity': self.identity,
                    'type': 'stop_iteration'
                }
            )
            self.logger.info('StopIteration')

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
                'rulesId': self.rules_id
            },
            partition=0
        )

    def stop(self):
        self.bool = False

    def __bool__(self):
        return self.bool
