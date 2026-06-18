#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import io
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from common.functools import singleton


@singleton
class S3(object):

    def __init__(self, *args, config=None, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        if config:
            self.client = boto3.client(
                's3',
                *args,
                config=Config(**config),
                **kwargs
            )
        else:
            self.client = boto3.client(
                's3',
                *args,
                **kwargs
            )

    def makedirs(self, path):
        try:
            self.client.head_bucket(Bucket=path)
        except ClientError:
            self.client.create_bucket(Bucket=path)

    def put_file(self, f, dir, name):
        if isinstance(f, io.BytesIO):
            f.seek(0)
        resp = self.client.put_object(
            Body=f,
            Bucket=dir,
            Key=name
        )
        return resp
