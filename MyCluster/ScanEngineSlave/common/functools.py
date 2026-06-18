#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import uuid
import logging
import importlib
import mmh3
from exstd.json import default
from threading import Lock


class Singleton(object):
    def __init__(self, cls):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cls = cls
        self.lock = Lock()
        self.cache = dict()

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
                self.logger.info('cache singleton')
                return self.cache[key]
            else:
                self.logger.info('create singleton')
                client = self.cls(*args, **kwargs)
                self.cache[key] = client
                return client
        except Exception:
            raise
        finally:
            self.lock.release()


def singleton(cls):
    return Singleton(cls)


def get_class(class_name):
    package, name = class_name.rsplit('.', 1)
    pack = importlib.import_module(package)
    cls = getattr(pack, name)
    return cls


def gen_key(**kwargs):
    return mmh3.hash_bytes(
        json.dumps(kwargs, ensure_ascii=False, indent=4).encode(
            'utf-8',
            errors='surrogateescape'
        )
    ).hex()


def boolean(truth):
    if truth:
        if isinstance(truth, str):
            if truth.lower() in {'false', 'null', 'undefined', 'no', '0'}:
                return False
            elif truth.isspace():
                return False
            else:
                return True
        else:
            return True
    else:
        return False
