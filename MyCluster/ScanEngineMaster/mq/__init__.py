#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import json
import traceback
from exstd.json import default
from common.functools import singleton
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from ex3rd.kafka.partitioner import CustomPartitioner


@singleton
class Producer(object):

    def __init__(self, *args, partitions=[], **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.partitions = partitions
        self.producer = KafkaProducer(
            *args,
            value_serializer=lambda v: json.dumps(
                v,
                ensure_ascii=False,
                indent=4,
                default=default
            ).encode(encoding='utf-8', errors='surrogateescape'),
            api_version=(2, 8, 1),
            **kwargs
        )
        if self.partitions:
            partitioner = CustomPartitioner(self.partitions)
            self.producer.config['partitioner'] = partitioner

    def send(self, *args, blocking=False, **kwargs):
        if blocking:
            self.sync_send(*args, **kwargs)
        else:
            self.async_send(*args, **kwargs)

    def flush(self):
        self.producer.flush()

    def send_all(self, partitions, *args, **kwargs):
        for partition in partitions:
            try:
                future = self.producer.send(
                    *args,
                    partition=partition,
                    **kwargs
                )
                future.get(600)
            except KafkaError:
                self.logger.error(traceback.format_exc())
                raise

    def sync_send(self, *args, **kwargs):
        try:
            future = self.producer.send(*args, **kwargs)
            future.get(600)
        except KafkaError:
            self.logger.error(traceback.format_exc())
            raise

    def async_send(self, *args, **kwargs):
        try:
            future = self.producer.send(*args, **kwargs)
            future.add_errback(self.errback)
        except KafkaError:
            self.logger.error(traceback.format_exc())

    def errback(self, exception):
        self.logger.error(exception)


@singleton
class Consumer(object):

    def __init__(self, *args, topic, partition, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.consumer = KafkaConsumer(
            *args,
            value_deserializer=lambda v: json.loads(
                v.decode(encoding='utf-8', errors='surrogateescape')
            ),
            api_version=(2, 8, 1),
            ** kwargs
        )
        self.tp = TopicPartition(topic, partition)
        self.consumer.assign(
            [
                self.tp
            ]
        )

    def get_lag(self):
        end_offsets = self.consumer.end_offsets([self.tp])[self.tp]
        committed = self.consumer.committed(self.tp)
        if end_offsets:
            self.logger.info('end_offsets: {}'.format(end_offsets))
            if committed:
                if isinstance(committed, int):
                    self.logger.info('committed: {}'.format(committed))
                    return end_offsets - committed
                else:
                    self.logger.info('committed: {}'.format(committed.offset))
                    return end_offsets - committed.offset
            else:
                return end_offsets
        else:
            return 0

    def commit_async(self):
        self.consumer.commit_async()

    def commit(self):
        try:
            self.consumer.commit()
        except KafkaError:
            self.logger.error(traceback.format_exc())
        except:
            self.logger.error(traceback.format_exc())

    def seek_to_last(self):
        try:
            end_offsets = self.consumer.end_offsets([self.tp])[self.tp]
            self.consumer.seek(self.tp, end_offsets - 1)
        except KafkaError:
            self.logger.error(traceback.format_exc())
        except:
            self.logger.error(traceback.format_exc())

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.consumer)
