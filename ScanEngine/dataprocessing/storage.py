#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import time
import uuid
from urllib.parse import quote
from common.globals import Variables
from transfer import S3
from mq import Producer


class MinioStorage(object):
    def __init__(
        self,
        identity,
        rules_id,
        task_id,
        partitions=[],
        url_enabled="0",
        url_distinguish_type="",
        url_distinguish_count="",
        **kwargs
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.identity = identity
        self.rules_id = rules_id
        self.task_id = task_id
        self.partitions = partitions
        self.host_id = os.environ.get('HOST_ID')
        self.url_enabled = url_enabled
        self.url_distinguish_type = url_distinguish_type
        self.url_distinguish_count = url_distinguish_count
        self.producer = Producer(
            partitions=self.partitions,
            client_id=identity,
            **self.variables.kafka['producer']
        )
        self.transfer = S3(**self.variables.minio)
        self.transfer.makedirs(self.identity)

    def __call__(self, node, data):
        """
        this method is a singledispatch by switch types including file, text, binary, and, string
        """
        if node['type'] == 'file':
            self.file(node, data)
        elif node['type'] == 'binary':
            self.binary(node, data)
        elif node['type'] == 'string':
            self.string(node, data)
        elif node['type'] == 'text':
            self.text(node, data)
        elif node['type'] == 'entirety':
            self.entirety(node, data)
        else:
            self.logger.error('unexpect type {}'.format(node['type']))

    def file(self, node, data):
        if 'diff' in node:
            node.data['diff'] = node['diff']
        node['uri'] = os.path.join(self.task_id, uuid.uuid4().hex)
        self.transfer.put_file(
            f=data,
            dir=self.identity,
            name=node['uri']
        )
        node['jobId'] = [self.identity]
        node['rulesId'] = self.rules_id
        node['taskId'] = self.task_id
        node['urlEnabled'] = self.url_enabled
        node['urlDistinguishType'] = self.url_distinguish_type
        node['urlDistinguishCount'] = self.url_distinguish_count
        node['storage'] = 'minio'
        node['se_time'] = int(time.time()*1000)
        node['host_id'] = self.host_id
        self.producer.send(
            topic=self.variables.kafka['file_topic'],
            value=node.data
        )

    def binary(self, node, data):
        if 'diff' in node:
            node.data['diff'] = node['diff']
        node['uri'] = os.path.join(self.task_id, uuid.uuid4().hex)
        self.transfer.put_file(
            f=data,
            dir=self.identity,
            name=node['uri']
        )
        node['jobId'] = [self.identity]
        node['rulesId'] = self.rules_id
        node['taskId'] = self.task_id
        node['storage'] = 'minio'
        node['size'] = len(data)
        node['se_time'] = int(time.time() * 1000)
        node['host_id'] = self.host_id
        self.producer.send(
            topic=self.variables.kafka['file_topic'],
            value=node.data
        )

    def string(self, node, data):
        if 'diff' in node:
            node.data['diff'] = node['diff']
        node['uri'] = os.path.join(self.task_id, uuid.uuid4().hex)
        node['jobId'] = [self.identity]
        node['rulesId'] = self.rules_id
        node['taskId'] = self.task_id
        node['storage'] = 'minio'
        node['size'] = len(data)
        node['se_time'] = int(time.time() * 1000)
        node['host_id'] = self.host_id
        node['contentPath'] = node['uri']
        self.transfer.put_file(
            f=data.value,
            dir=self.identity,
            name=node['uri']
        )
        self.producer.send(
            topic=self.variables.kafka['text_topic'],
            value=node.data
        )

    def text(self, node, data):
        if 'diff' in node:
            node.data['diff'] = node['diff']
        node['jobId'] = [self.identity]
        node['rulesId'] = self.rules_id
        node['taskId'] = self.task_id
        node['storage'] = 'minio'
        node['se_time'] = int(time.time() * 1000)
        node['host_id'] = self.host_id
        self.producer.send(
            topic=self.variables.kafka['text_topic'],
            value=node.data
        )

    def entirety(self, node, data):
        if 'diff' in node:
            node.data['diff'] = node['diff']
        if node['name'].lower().endswith('.gdb'):
            node['uri'] = os.path.join(
                self.task_id,
                uuid.uuid4().hex,
                '{}{}'.format(uuid.uuid4().hex, '.gdb')
            )
        else:
            node['uri'] = os.path.join(self.task_id, uuid.uuid4().hex)
        node['jobId'] = [self.identity]
        node['rulesId'] = self.rules_id
        node['taskId'] = self.task_id
        node['urlEnabled'] = self.url_enabled
        node['urlDistinguishType'] = self.url_distinguish_type
        node['urlDistinguishCount'] = self.url_distinguish_count
        node['storage'] = 'minio'
        node['se_time'] = int(time.time() * 1000)
        node['host_id'] = self.host_id
        for child, f in zip(node['children'], data):
            self.transfer.put_file(
                f=f,
                dir=self.identity,
                name=quote(
                    child['path'].replace(node['path'], node['uri']),
                    encoding='utf-8',
                    errors='surrogateescape'
                )
            )
        self.producer.send(
            topic=self.variables.kafka['file_topic'],
            value=node.data
        )
