#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
from platform import node


class WPSError(Exception):

    def __init__(self, resp):
        self.resp = resp
        self.code = resp.get('code')
        self.message = resp.get('msg')

    def __str__(self):
        return json.dumps(self.resp, ensure_ascii=False, indent=4)


class ObsError(Exception):

    def __init__(self, resp):
        self.status = resp.status
        self.reason = resp.reason
        self.requestId = resp.requestId
        self.errorCode = resp.errorCode
        self.errorMessage = resp.errorMessage

    def __str__(self):
        parameters = {
            'status': self.status,
            'reason': self.reason,
            'requestId': self.requestId,
            'errorCode': self.errorCode,
            'errorMessage': self.errorMessage
        }
        return json.dumps(parameters, ensure_ascii=False, indent=4)


class HBASERESTError(Exception):

    def __init__(self, resp):
        self.status = resp['status_code']
        self.reason = resp['error']

    def __str__(self):
        parameters = {
            'status': self.status,
            'reason': self.reason,
        }
        return json.dumps(parameters, ensure_ascii=False, indent=4)


class FilterError(Exception):

    def __init__(self, item):
        self.item = item

    @property
    def status(self):
        if self.item == 'size':
            return '540512'
        elif self.item in ('path', 'name'):
            return '540513'
        elif self.item == 'timestamp':
            return '540514'
        else:
            return '-1'

    def __str__(self):
        return 'filtered by {}'.format(self.item)


class LotusError(Exception):

    def __init__(self, code):
        self.code = code

    def __str__(self):
        return 'code is {}'.format(self.code)


class IguoError(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return str(self.message)


class UDSError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return str(self.message)


class EYouError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return str(self.message)


class UDSRESTError(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return str(self.message)
