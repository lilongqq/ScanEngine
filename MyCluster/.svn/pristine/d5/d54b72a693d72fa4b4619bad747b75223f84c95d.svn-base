#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import logging
import os.path
import copy
import yaml
import redis
from functools import cached_property

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
from .functools import singleton
from .crypto import Asymmetric


@singleton
class Variables(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        abspath = os.path.abspath(sys.argv[0])
        self.root = abspath[:abspath.find('/ScanEngine')]
        with open(os.path.join(self.root, 'config/ScanEngine.yaml'), 'r') as f:
            self.conf = yaml.load(f, Loader=Loader)
        self.crypto = Asymmetric(os.path.join(self.certs, 'server-key.pem'))

    @property
    def logging(self):
        config = self.conf['logging']
        filename = config['handlers']['file']['filename']
        if filename.startswith('/'):
            return self.conf['logging']
        else:
            config['handlers']['file']['filename'] = os.path.join(
                self.root,
                filename
            )
            return config

    @property
    def executor(self):
        return self.conf['executor']

    @property
    def dbp(self):
        return self.conf['dbp']

    @property
    def hbase(self):
        return self.conf['hbase']

    @property
    def strategy(self):
        return os.path.join(self.root, 'strategy')

    @property
    def certs(self):
        return os.path.join(self.root, 'certs')

    @property
    def kafka(self):
        return self.conf['kafka']

    @property
    def email(self):
        return self.conf['email']

    @cached_property
    def redis(self):
        config = copy.deepcopy(self.conf['redis'])
        config['auth']['password'] = self.crypto.decrypt(
            config['auth']['password']
        )
        return config

    @property
    def lotus(self):
        return self.conf['lotus']

    @property
    def differator(self):
        return self.conf['differator']

    @property
    def flask(self):
        return self.conf['flask']

    @property
    def s3(self):
        return self.conf['s3']

    @cached_property
    def minio(self):
        config = copy.deepcopy(self.conf['minio'])
        config['aws_access_key_id'] = self.crypto.decrypt(
            config['aws_access_key_id']
        )
        config['aws_secret_access_key'] = self.crypto.decrypt(
            config['aws_secret_access_key']
        )
        return config

    @property
    def ftp(self):
        return self.conf['ftp']

    @cached_property
    def sm4_key(self):
        config = copy.deepcopy(self.conf['sm4_key'])
        config['key'] = self.crypto.decrypt(
            config['key']
        )
        return config


Redis = singleton(redis.Redis)
