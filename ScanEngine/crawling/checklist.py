#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import copy
import logging
from mq import Producer
from common.globals import Redis, Variables


class Test(object):

    def __init__(
            self,
            identity,
            rules_id,
            task_id,
            rule_list=[]
    ):
        self.identity = identity
        self.rules_id = rules_id
        self.task_id = task_id
        self.host_id = os.environ.get('HOST_ID')
        self.variables = Variables()
        self.client = Redis(**self.variables.redis['auth'])
        self.producer = Producer(
            **self.variables.kafka['producer']
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rule_list = rule_list
        base_blacklist = self.variables.redis.get('blacklist', 'EVENT_BLACK_LIST')
        base_whitelist = self.variables.redis.get('whitelist', 'EVENT_WHITE_LIST')
        if rule_list:
            self.checklist_keys = [
                ('{}:{}'.format(base_blacklist, rule_id), '{}:{}'.format(base_whitelist, rule_id))
                for rule_id in rule_list
            ]
        else:
            self.checklist_keys = [(base_blacklist, base_whitelist)]
        self.processed_template = {
            "jobId": "d6490558f5b243fb90220c51e4ecc889",
            "path": "",
            "taskId": "9aaf9f4a3e5440c59aa124ae8d446af6",
            "rulesId": "d9486860c39e353eadc9fcf2662ecfe7",
            "uri": "",
            "count": "1",
            "size": "158",
            "type": "string",
            "se_time": "1773626269728",
            "pf_time": "1773626269728",
            "pe_time": "1773626420069",
            "diff": "ef2ced2335158173884bc58e5da5d2ac",
            "isAttach": "0",
            "host_id": "",
            "md5": "ef2ced2335158173884bc58e5da5d2ac",
            "msgType": "",
            "matchResult": "",
            "status": "0"
        }
        self.event_template = {
            'jobId': [
                {
                    'taskId': '2ad8e302ff9b4838973ee3d759ec7a26',
                    'id': 'd493d9a5071f4875823c4a688b1bd70c',
                    'name': '',
                    'urlFullPath': '',
                    'urlContentPath': '',
                    'actionType': '',
                    'rule': [
                        {
                            'id': 'd348625n43242l432m43242',
                            'rulesId': '735157469234344280ad598b9e240509',
                            'subsystem': '0',
                            'type': '0',
                            'severity': '1',
                            'resourceType': '',
                            'actionName': '0',
                            'actionType': '0',
                            'numOfMatches': '1',
                            'secretRate': '99.000000',
                            'eventType': '1',
                            'ruleName': '',
                            'classifierMatches': [
                                {
                                    'id': '23674',
                                    'name': '',
                                    'numOfMatches': '1',
                                    'unMaskVal': '匹配到黑名单事件',
                                    'unMasked': '匹配到黑名单事件',
                                    'matchPos': '8',
                                    'fileMatchPos': '1',
                                    'excludePattern': '',
                                    'excludePhrase': '',
                                    'databaseMatchRows': ''
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    def __call__(self, node):
        if 'md5' in node:
            pipeline = self.client.pipeline()
            for blacklist_key, whitelist_key in self.checklist_keys:
                pipeline.sismember(blacklist_key, node['md5'])
                pipeline.sismember(whitelist_key, node['md5'])
            results = pipeline.execute()
            black = any(results[i] for i in range(0, len(results), 2))
            white = any(results[i] for i in range(1, len(results), 2))
            if black:
                matched_rules = [self.rule_list[i] for i in range(len(self.rule_list)) if results[i * 2]]
                if 'diff' in node:
                    node.data['diff'] = node['diff']
                node['jobId'] = [self.identity]
                node['rulesId'] = self.rules_id
                node['taskId'] = self.task_id
                node['se_time'] = int(time.time()*1000)
                node['host_id'] = self.host_id
                event = copy.deepcopy(self.event_template)
                event['jobId'][0]['id'] = self.identity
                event['jobId'][0]['taskId'] = self.task_id
                event['jobId'][0]['rule'][0]['rulesId'] = self.rules_id
                event['file'] = node.data
                event['ruleList'] = matched_rules
                self.producer.send(
                    topic=self.variables.kafka['event_topic'],
                    value=event
                )
                processed = copy.deepcopy(self.processed_template)
                processed['jobId'] = self.identity
                processed['taskId'] = self.task_id
                processed['rulesId'] = self.rules_id
                processed['path'] = node['path']
                processed['size'] = node['size']
                processed['type'] = node['type']
                processed['se_time'] = node['se_time']
                processed['pf_time'] = node['se_time']
                processed['pe_time'] = node['se_time']
                processed['diff'] = node['diff']
                processed['host_id'] = self.host_id
                processed['md5'] = node['md5']
                processed['count'] = node.get('count', 1)
                processed['ruleList'] = matched_rules
                self.producer.send(
                    topic=self.variables.kafka['processed'],
                    value=processed
                )
                return False
            elif white:
                matched_rules = [self.rule_list[i] for i in range(len(self.rule_list)) if results[i * 2 + 1]]
                if 'diff' in node:
                    node.data['diff'] = node['diff']
                node['se_time'] = int(time.time()*1000)
                processed = copy.deepcopy(self.processed_template)
                processed['jobId'] = self.identity
                processed['taskId'] = self.task_id
                processed['rulesId'] = self.rules_id
                processed['path'] = node['path']
                processed['size'] = node['size']
                processed['type'] = node['type']
                processed['se_time'] = node['se_time']
                processed['pf_time'] = node['se_time']
                processed['pe_time'] = node['se_time']
                processed['diff'] = node['diff']
                processed['host_id'] = self.host_id
                processed['md5'] = node['md5']
                processed['count'] = node.get('count', 1)
                processed['ruleList'] = matched_rules
                self.producer.send(
                    topic=self.variables.kafka['processed'],
                    value=processed
                )
                return False
            else:
                return True
        else:
            return True


class Checklist(object):

    def __init__(
            self,
            identity,
            rules_id,
            task_id,
            target,
            rule_list=[]
        ):
        self.target = target
        self.test = Test(
            identity,
            rules_id,
            task_id,
            rule_list=rule_list
        )
    
    def __call__(self, node, data):
        if self.test(node):
            self.target(node, data)
