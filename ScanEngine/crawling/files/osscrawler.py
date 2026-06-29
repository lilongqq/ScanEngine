#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import hashlib
import logging
from datetime import datetime
import oss2
import boto3
from botocore.client import Config
from collections import UserDict
from collections.abc import MutableMapping
from obs import ObsClient
from qcloud_cos import CosConfig, CosS3Client
import io
from tempfile import SpooledTemporaryFile
import os.path
from functools import singledispatchmethod, partial
from crawling.interface import Iterator, Client
from exceptions import ObsError
from common.globals import Variables
from common.conntool import weak_cache
from common.functools import gen_key


class OSSDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['last_write_time']
        elif item == 'diff':
            return gen_key(
                path=self.data.get('path'),
                last_write_time=self.data.get('last_write_time')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return {'last_write_time', 'path'}.issubset(self.data)
        else:
            return item in self.data


class AliCloud(Client):

    def __init__(self, ak, sk, endpoint, **kwargs):
        self.auth = oss2.Auth(ak, sk)
        self.endpoint = endpoint
        self.service = oss2.Service(self.auth, self.endpoint)

    @staticmethod
    def __bucket_name_and_prefix(path):
        path_tuple = path.split('/', 1)
        if len(path_tuple) == 2:
            bucket_name, prefix = path_tuple
        else:
            bucket_name = path
            prefix = ''
        return bucket_name, prefix

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if node and (
                path := node.get('path', node['name'])
        ):
            bucket_name, prefix = self.__bucket_name_and_prefix(path)
            bucket = oss2.Bucket(self.auth, self.endpoint, bucket_name)
            if 'next_marker' in node:
                resp = bucket.list_objects(
                    prefix=prefix,
                    delimiter='/',
                    marker=node['next_marker']
                )
            else:
                resp = bucket.list_objects(prefix=prefix, delimiter='/')
            for item in resp.prefix_list:
                node = dict()
                node['type'] = 'dir'
                node['name'] = item.replace(prefix, '')
                node['path'] = os.path.join(bucket_name, item)
                node['key'] = item
                node['children'] = []
                nodes.append(node)
            for item in resp.object_list:
                if not item.key.endswith('/'):
                    node = dict()
                    node['type'] = 'file'
                    node['name'] = os.path.basename(item.key)
                    node['path'] = os.path.join(bucket_name, item.key)
                    node['etag'] = item.etag
                    node['key'] = item.key
                    node['last_write_time'] = item.last_modified
                    node['size'] = item.size
                    node['storage_class'] = item.storage_class
                    node['storage_type'] = item.type
                    node['display_name'] = item.owner.display_name
                    node['owner_id'] = item.owner.id
                    nodes.append(node)
            if resp.is_truncated:
                node = dict()
                node['type'] = 'file'
                node['name'] = '...'
                node['path'] = path
                node['next_marker'] = resp.next_marker
                node['sibling'] = []
                nodes.append(node)
        else:
            for bucket in oss2.BucketIterator(self.service):
                node = dict()
                node['type'] = 'dir'
                node['name'] = bucket.name
                node['path'] = bucket.name
                node['creation_date'] = bucket.creation_date
                node['extranet_endpoint'] = bucket.extranet_endpoint
                node['intranet_endpoint'] = bucket.intranet_endpoint
                node['location'] = bucket.location
                node['storage_class'] = bucket.storage_class
                node['children'] = []
                nodes.append(node)
        return nodes

    @get_nodes.register(str)
    def _(self, path):
        nodes = list()
        if path:
            bucket_name, prefix = self.__bucket_name_and_prefix(path)
            bucket = oss2.Bucket(self.auth, self.endpoint, bucket_name)
            resp = bucket.list_objects(prefix=prefix, delimiter='/')
            for item in resp.prefix_list:
                node = dict()
                node['type'] = 'dir'
                node['name'] = item.replace(prefix, '')
                node['path'] = os.path.join(bucket_name, item)
                node['key'] = item
                node['children'] = []
                nodes.append(node)
            for item in resp.object_list:
                if not item.key.endswith('/'):
                    node = dict()
                    node['type'] = 'file'
                    node['name'] = os.path.basename(item.key)
                    node['path'] = os.path.join(bucket_name, item.key)
                    node['etag'] = item.etag
                    node['key'] = item.key
                    node['last_write_time'] = item.last_modified
                    node['size'] = item.size
                    node['storage_class'] = item.storage_class
                    node['storage_type'] = item.type
                    node['display_name'] = item.owner.display_name
                    node['owner_id'] = item.owner.id
                    nodes.append(node)
            if resp.is_truncated:
                node = dict()
                node['type'] = 'file'
                node['name'] = '...'
                node['path'] = path
                node['next_marker'] = resp.next_marker
                node['sibling'] = []
                nodes.append(node)
        else:
            for bucket in oss2.BucketIterator(self.service):
                node = dict()
                node['type'] = 'dir'
                node['name'] = bucket.name
                node['path'] = bucket.name
                node['creation_date'] = bucket.creation_date
                node['extranet_endpoint'] = bucket.extranet_endpoint
                node['intranet_endpoint'] = bucket.intranet_endpoint
                node['location'] = bucket.location
                node['storage_class'] = bucket.storage_class
                node['children'] = []
                nodes.append(node)
        return nodes

    @singledispatchmethod
    def get_file(self, *args, **kwargs):
        raise NotImplemented

    @get_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        bucket = oss2.Bucket(self.auth, self.endpoint, bucket_name)
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        resp = bucket.get_object(key)
        for chunk in resp:
            f.write(chunk)
        f.seek(0)
        return f

    @get_file.register(str)
    def _(self, path):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        bucket = oss2.Bucket(self.auth, self.endpoint, bucket_name)
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        resp = bucket.get_object(key)
        for chunk in resp:
            f.write(chunk)
        f.seek(0)
        return f

    @singledispatchmethod
    def delete_file(self, *args, **kwargs):
        raise NotImplemented

    @delete_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        bucket = oss2.Bucket(self.auth, self.endpoint, bucket_name)
        resp = bucket.delete_object(key)
        return resp

    @delete_file.register(str)
    def _(self, path):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        bucket = oss2.Bucket(self.auth, self.endpoint, bucket_name)
        resp = bucket.delete_object(key)
        return resp

    @singledispatchmethod
    def put_file(self, *args, **kwargs):
        raise NotImplemented

    @put_file.register(MutableMapping)
    def _(self, node, f):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        bucket = oss2.Bucket(self.auth, self.endpoint, bucket_name)
        resp = bucket.put_object(key, f)
        return resp

    @put_file.register(str)
    def _(self, path, f):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        bucket = oss2.Bucket(self.auth, self.endpoint, bucket_name)
        resp = bucket.put_object(key, f)
        return resp

    def empty_file(self, node):
        f = io.BytesIO()
        self.put_file(node, f)

    def store_file(self, node, f):
        path = os.path.join(node.get('sepPath', ''), node['name'])
        self.put_file(path, f)

    def __bool__(self):
        return True

    def __del__(self):
        return True


class AliCloudBatch(AliCloud):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@weak_cache
class AliCloudOne(AliCloud):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class HuaWeiCloud(Client):

    def __init__(self, ak, sk, endpoint, **kwargs):
        self.client = ObsClient(
            access_key_id=ak,
            secret_access_key=sk,
            server=endpoint,
            max_redirect_count=1
        )

    @staticmethod
    def __bucket_name_and_prefix(path):
        path_tuple = path.split('/', 1)
        if len(path_tuple) == 2:
            bucket_name, prefix = path_tuple
        else:
            bucket_name = path
            prefix = None
        return bucket_name, prefix

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if node and (
                path := node.get('path', node['name'])
        ):
            bucket_name, prefix = self.__bucket_name_and_prefix(path)
            if 'next_marker' in node:
                resp = self.client.listObjects(
                    bucketName=bucket_name,
                    prefix=prefix,
                    delimiter='/',
                    marker=node['next_marker']
                )
            else:
                resp = self.client.listObjects(
                    bucketName=bucket_name,
                    prefix=prefix,
                    delimiter='/'
                )
            if resp.status >= 300:
                raise ObsError(
                    resp
                )
            for item in resp.body.commonPrefixs:
                node = dict()
                node['type'] = 'dir'
                if prefix:
                    node['name'] = item.prefix.replace(prefix, '')
                else:
                    node['name'] = item.prefix
                node['path'] = os.path.join(bucket_name, item.prefix)
                node['key'] = item.prefix
                node['children'] = []
                nodes.append(node)
            for item in resp.body.contents:
                node = dict()
                node['type'] = 'file'
                node['name'] = os.path.basename(item.key)
                node['path'] = os.path.join(bucket_name, item.key)
                node['etag'] = item.etag
                node['key'] = item.key
                node['last_write_time'] = datetime.strptime(
                    item.lastModified,
                    '%Y/%m/%d %H:%M:%S'
                ).timestamp()
                node['size'] = item.size
                node['storage_class'] = item.storageClass
                node['owner_id'] = item.owner.owner_id
                nodes.append(node)
            if resp.body.is_truncated:
                node = dict()
                node['type'] = 'file'
                node['name'] = '...'
                node['path'] = path
                node['next_marker'] = resp.body.next_marker
                node['sibling'] = []
                nodes.append(node)
        else:
            resp = self.client.listBuckets()
            if resp.status >= 300:
                raise ObsError(
                    resp
                )
            for bucket in resp.body.buckets:
                node = dict()
                node['type'] = 'dir'
                node['creation_date'] = bucket.create_date
                node['name'] = bucket.name
                node['path'] = bucket.name
                node['location'] = bucket.location
                node['children'] = []
                nodes.append(node)
        return nodes

    @get_nodes.register(str)
    def _(self, path):
        nodes = list()
        if path:
            bucket_name, prefix = self.__bucket_name_and_prefix(path)
            resp = self.client.listObjects(
                bucketName=bucket_name,
                prefix=prefix,
                delimiter='/'
            )
            if resp.status >= 300:
                raise ObsError(
                    resp
                )
            for item in resp.body.commonPrefixs:
                node = dict()
                node['type'] = 'dir'
                if prefix:
                    node['name'] = item.prefix.replace(prefix, '')
                else:
                    node['name'] = item.prefix
                node['path'] = os.path.join(bucket_name, item.prefix)
                node['key'] = item.prefix
                node['children'] = []
                nodes.append(node)
            for item in resp.body.contents:
                node = dict()
                node['type'] = 'file'
                node['name'] = os.path.basename(item.key)
                node['path'] = os.path.join(bucket_name, item.key)
                node['etag'] = item.etag
                node['key'] = item.key
                node['last_write_time'] = datetime.strptime(
                    item.lastModified,
                    '%Y/%m/%d %H:%M:%S'
                ).timestamp()
                node['size'] = item.size
                node['storage_class'] = item.storageClass
                node['owner_id'] = item.owner.owner_id
                nodes.append(node)
            if resp.body.is_truncated:
                node = dict()
                node['type'] = 'file'
                node['name'] = '...'
                node['path'] = path
                node['next_marker'] = resp.body.next_marker
                node['sibling'] = []
                nodes.append(node)
        else:
            resp = self.client.listBuckets()
            if resp.status >= 300:
                raise ObsError(
                    resp
                )
            for bucket in resp.body.buckets:
                node = dict()
                node['type'] = 'dir'
                node['creation_date'] = bucket.create_date
                node['name'] = bucket.name
                node['path'] = bucket.name
                node['location'] = bucket.location
                node['children'] = []
                nodes.append(node)
        return nodes

    @singledispatchmethod
    def get_file(self, *args, **kwargs):
        raise NotImplemented

    @get_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.getObject(
            bucketName=bucket_name,
            objectKey=key,
            loadStreamInMemory=True
        )
        if resp.status >= 300:
            raise ObsError(
                resp
            )
        f = io.BytesIO(resp.body.buffer)
        return f

    @get_file.register(str)
    def _(self, path):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.getObject(
            bucketName=bucket_name,
            objectKey=key,
            loadStreamInMemory=True
        )
        if resp.status >= 300:
            raise ObsError(
                resp
            )
        f = io.BytesIO(resp.body.buffer)
        return f

    @singledispatchmethod
    def delete_file(self, *args, **kwargs):
        raise NotImplemented

    @delete_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.deleteObject(bucketName=bucket_name, objectKey=key)
        if resp.status >= 300:
            raise ObsError(
                resp
            )
        return resp

    @delete_file.register(str)
    def _(self, path):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.deleteObject(bucketName=bucket_name, objectKey=key)
        if resp.status >= 300:
            raise ObsError(
                resp
            )
        return resp

    @singledispatchmethod
    def put_file(self, *args, **kwargs):
        raise NotImplemented

    @put_file.register(MutableMapping)
    def _(self, node, f):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.putObject(
            bucketName=bucket_name,
            objectKey=key,
            content=f
        )
        if resp.status >= 300:
            raise ObsError(
                resp
            )
        return resp

    @put_file.register(str)
    def _(self, path, f):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.putObject(
            bucketName=bucket_name,
            objectKey=key,
            content=f
        )
        if resp.status >= 300:
            raise ObsError(
                resp
            )
        return resp

    def empty_file(self, node):
        f = io.BytesIO()
        self.put_file(node, f)

    def store_file(self, node, f):
        path = os.path.join(node.get('sepPath', ''), node['name'])
        self.put_file(path, f)

    def __bool__(self):
        return True

    def __del__(self):
        return True


class HuaWeiCloudBatch(HuaWeiCloud):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@weak_cache
class HuaWeiCloudOne(HuaWeiCloud):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class TencentCloud(Client):

    def __init__(self, ak, sk, endpoint, **kwargs):
        self.config = CosConfig(SecretId=ak, SecretKey=sk, Endpoint=endpoint)
        self.client = CosS3Client(self.config)

    @staticmethod
    def __bucket_name_and_prefix(path):
        path_tuple = path.split('/', 1)
        if len(path_tuple) == 2:
            bucket_name, prefix = path_tuple
        else:
            bucket_name = path
            prefix = ''
        return bucket_name, prefix

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if node and (
                path := node.get('path', node['name'])
        ):
            bucket_name, prefix = self.__bucket_name_and_prefix(path)
            if 'next_marker' in node:
                resp = self.client.list_objects(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    Delimiter='/',
                    Marker=node['next_marker']
                )
            else:
                resp = self.client.list_objects(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    Delimiter='/'
                )
            if 'CommonPrefixes' in resp:
                for item in resp['CommonPrefixes']:
                    node = dict()
                    node['type'] = 'dir'
                    node['name'] = item['Prefix'].replace(prefix, '')
                    node['path'] = os.path.join(bucket_name, item['Prefix'])
                    node['key'] = item['Prefix']
                    node['children'] = []
                    nodes.append(node)
            if 'Contents' in resp:
                for item in resp['Contents']:
                    if not item['Key'].endswith('/'):
                        node = dict()
                        node['type'] = 'file'
                        node['name'] = os.path.basename(item['Key'])
                        node['path'] = os.path.join(bucket_name, item['Key'])
                        node['etag'] = item['ETag']
                        node['key'] = item['Key']
                        node['last_write_time'] = datetime.strptime(
                            item['LastModified'], '%Y-%m-%dT%H:%M:%S.%f%z'
                        ).timestamp()
                        node['size'] = item['Size']
                        node['storage_class'] = item['StorageClass']
                        node['display_name'] = item['Owner']['DisplayName']
                        node['owner_id'] = item['Owner']['ID']
                        nodes.append(node)
            if resp['IsTruncated'] == 'true':
                node = dict()
                node['type'] = 'file'
                node['name'] = '...'
                node['path'] = path
                node['next_marker'] = resp['NextMarker']
                node['sibling'] = []
                nodes.append(node)
        else:
            resp = self.client.list_buckets()
            for bucket in resp['Buckets']['Bucket']:
                node = dict()
                node['type'] = 'dir'
                node['creation_date'] = bucket['CreationDate']
                node['name'] = bucket['Name']
                node['path'] = bucket['Name']
                node['location'] = bucket['Location']
                node['children'] = []
                nodes.append(node)
        return nodes

    @get_nodes.register(str)
    def _(self, path):
        nodes = list()
        if path:
            bucket_name, prefix = self.__bucket_name_and_prefix(path)
            resp = self.client.list_objects(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            if 'CommonPrefixes' in resp:
                for item in resp['CommonPrefixes']:
                    node = dict()
                    node['type'] = 'dir'
                    node['name'] = item['Prefix'].replace(prefix, '')
                    node['path'] = os.path.join(bucket_name, item['Prefix'])
                    node['key'] = item['Prefix']
                    node['children'] = []
                    nodes.append(node)
            if 'Contents' in resp:
                for item in resp['Contents']:
                    if not item['Key'].endswith('/'):
                        node = dict()
                        node['type'] = 'file'
                        node['name'] = os.path.basename(item['Key'])
                        node['path'] = os.path.join(bucket_name, item['Key'])
                        node['etag'] = item['ETag']
                        node['key'] = item['Key']
                        node['last_write_time'] = datetime.strptime(
                            item['LastModified'], '%Y-%m-%dT%H:%M:%S.%f%z'
                        ).timestamp()
                        node['size'] = item['Size']
                        node['storage_class'] = item['StorageClass']
                        node['display_name'] = item['Owner']['DisplayName']
                        node['owner_id'] = item['Owner']['ID']
                        nodes.append(node)
            if resp['IsTruncated'] == 'true':
                node = dict()
                node['type'] = 'file'
                node['name'] = '...'
                node['path'] = path
                node['next_marker'] = resp['NextMarker']
                node['sibling'] = []
                nodes.append(node)
        else:
            resp = self.client.list_buckets()
            for bucket in resp['Buckets']['Bucket']:
                node = dict()
                node['type'] = 'dir'
                node['creation_date'] = bucket['CreationDate']
                node['name'] = bucket['Name']
                node['path'] = bucket['Name']
                node['location'] = bucket['Location']
                node['children'] = []
                nodes.append(node)
        return nodes

    @singledispatchmethod
    def get_file(self, *args, **kwargs):
        raise NotImplemented

    @get_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.get_object(Bucket=bucket_name, Key=key)
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        for chunk in resp['Body']:
            f.write(chunk)
        f.seek(0)
        return f

    @get_file.register(MutableMapping)
    def _(self, path):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.get_object(Bucket=bucket_name, Key=key)
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        for chunk in resp['Body']:
            f.write(chunk)
        f.seek(0)
        return f

    @singledispatchmethod
    def delete_file(self, *args, **kwargs):
        raise NotImplemented

    @delete_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.delete_object(Bucket=bucket_name, Key=key)
        return resp

    @delete_file.register(str)
    def _(self, path):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.delete_object(Bucket=bucket_name, Key=key)
        return resp

    @singledispatchmethod
    def put_file(self, *args, **kwargs):
        raise NotImplemented

    @put_file.register(MutableMapping)
    def _(self, node, f):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.put_object(Bucket=bucket_name, Body=f, Key=key)
        return resp

    @put_file.register(str)
    def _(self, path, f):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.put_object(Bucket=bucket_name, Body=f, Key=key)
        return resp

    def empty_file(self, node):
        f = io.BytesIO()
        self.put_file(node, f)

    def store_file(self, node, f):
        path = os.path.join(node.get('sepPath', ''), node['name'])
        self.put_file(path, f)

    def __bool__(self):
        return True

    def __del__(self):
        return True


class TencentCloudBatch(TencentCloud):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@weak_cache
class TencentCloudOne(TencentCloud):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class S3(Client):

    def __init__(
            self,
            ak,
            sk,
            endpoint,
            addressing_style='auto',  # addressing_style='virtual', addressing_style='path'
            **kwargs
    ):
        self.variables = Variables()
        self.config = Config(
            s3={
                'addressing_style': addressing_style
            },
            **self.variables.s3['config']
        )
        self.client = boto3.client(
            's3',
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            endpoint_url=endpoint,
            verify=False,
            config=self.config
        )

    @staticmethod
    def __bucket_name_and_prefix(path):
        path_tuple = path.split('/', 1)
        if len(path_tuple) == 2:
            bucket_name, prefix = path_tuple
        else:
            bucket_name = path
            prefix = ''
        return bucket_name, prefix

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if node and (
                path := node.get('path', node['name'])
        ):
            bucket_name, prefix = self.__bucket_name_and_prefix(path)
            if 'next_marker' in node:
                resp = self.client.list_objects(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    Delimiter='/',
                    Marker=node['next_marker']
                )
            else:
                resp = self.client.list_objects(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    Delimiter='/'
                )
            if 'CommonPrefixes' in resp:
                for item in resp['CommonPrefixes']:
                    node = dict()
                    node['type'] = 'dir'
                    node['name'] = item['Prefix'].replace(prefix, '')
                    node['path'] = '/'.join([bucket_name, item['Prefix']])
                    node['key'] = item['Prefix']
                    node['children'] = []
                    nodes.append(node)
            if 'Contents' in resp:
                for item in resp['Contents']:
                    if not item['Key'].endswith('/'):
                        node = dict()
                        node['type'] = 'file'
                        node['name'] = os.path.basename(item['Key'])
                        node['path'] = '/'.join([bucket_name, item['Key']])
                        node['etag'] = item['ETag']
                        node['key'] = item['Key']
                        node['last_write_time'] = item['LastModified'].timestamp()
                        node['size'] = item['Size']
                        node['storage_class'] = item['StorageClass']
                        node['display_name'] = item['Owner']['DisplayName']
                        node['owner_id'] = item['Owner']['ID']
                        nodes.append(node)
            if resp['IsTruncated']:
                node = dict()
                node['type'] = 'file'
                node['name'] = '...'
                node['path'] = path
                node['next_marker'] = resp['NextMarker']
                node['sibling'] = []
                nodes.append(node)
        else:
            resp = self.client.list_buckets()
            for bucket in resp['Buckets']:
                node = dict()
                node['type'] = 'dir'
                node['creation_date'] = bucket['CreationDate'].isoformat()
                node['name'] = bucket['Name']
                node['path'] = bucket['Name']
                node['children'] = []
                try:
                    resp = self.client.get_bucket_location(
                        Bucket=bucket['Name'])
                except:
                    node['location'] = ''
                else:
                    node['location'] = resp['LocationConstraint']
                nodes.append(node)
        return nodes

    @get_nodes.register(str)
    def _(self, path):
        nodes = list()
        if path:
            bucket_name, prefix = self.__bucket_name_and_prefix(path)
            resp = self.client.list_objects(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            if 'CommonPrefixes' in resp:
                for item in resp['CommonPrefixes']:
                    node = dict()
                    node['type'] = 'dir'
                    node['name'] = item['Prefix'].replace(prefix, '')
                    node['path'] = '/'.join([bucket_name, item['Prefix']])
                    node['key'] = item['Prefix']
                    node['children'] = []
                    nodes.append(node)
            if 'Contents' in resp:
                for item in resp['Contents']:
                    if not item['Key'].endswith('/'):
                        node = dict()
                        node['type'] = 'file'
                        node['name'] = os.path.basename(item['Key'])
                        node['path'] = '/'.join([bucket_name, item['Key']])
                        node['etag'] = item['ETag']
                        node['key'] = item['Key']
                        node['last_write_time'] = item['LastModified'].timestamp()
                        node['size'] = item['Size']
                        node['storage_class'] = item['StorageClass']
                        node['display_name'] = item['Owner']['DisplayName']
                        node['owner_id'] = item['Owner']['ID']
                        nodes.append(node)
            if resp['IsTruncated']:
                node = dict()
                node['type'] = 'file'
                node['name'] = '...'
                node['path'] = path
                node['next_marker'] = resp['NextMarker']
                node['sibling'] = []
                nodes.append(node)
        else:
            resp = self.client.list_buckets()
            for bucket in resp['Buckets']:
                node = dict()
                node['type'] = 'dir'
                node['creation_date'] = bucket['CreationDate'].isoformat()
                node['name'] = bucket['Name']
                node['path'] = bucket['Name']
                node['children'] = []
                try:
                    resp = self.client.get_bucket_location(
                        Bucket=bucket['Name'])
                except:
                    node['location'] = ''
                else:
                    node['location'] = resp['LocationConstraint']
                nodes.append(node)
        return nodes

    @singledispatchmethod
    def get_file(self, *args, **kwargs):
        raise NotImplemented

    @get_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.get_object(Bucket=bucket_name, Key=key)
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        for chunk in resp['Body']:
            f.write(chunk)
        f.seek(0)
        return f

    @get_file.register(str)
    def _(self, path):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.get_object(Bucket=bucket_name, Key=key)
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        for chunk in resp['Body']:
            f.write(chunk)
        f.seek(0)
        return f

    @singledispatchmethod
    def delete_file(self, *args, **kwargs):
        raise NotImplemented

    @delete_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.delete_object(Bucket=bucket_name, Key=key)
        return resp

    @delete_file.register(str)
    def _(self, path):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.delete_object(Bucket=bucket_name, Key=key)
        return resp

    @singledispatchmethod
    def put_file(self, *args, **kwargs):
        raise NotImplemented

    @put_file.register(MutableMapping)
    def _(self, node, f):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.put_object(Bucket=bucket_name, Body=f, Key=key)
        return resp

    @put_file.register(str)
    def _(self, path, f):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.put_object(Bucket=bucket_name, Body=f, Key=key)
        return resp

    def empty_file(self, node):
        f = io.BytesIO()
        self.put_file(node, f)

    def store_file(self, node, f):
        path = os.path.join(node.get('sepPath', ''), node['name'])
        self.put_file(path, f)

    def __bool__(self):
        return True

    def __del__(self):
        return True


@weak_cache
class S3One(S3):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class AWSCloud(Client):

    def __init__(self, ak, sk, endpoint=None, region=None, verify=True, **kwargs):
        self.variables = Variables()
        self.config = Config(
            s3={
                'addressing_style': 'auto'
            },
            **self.variables.s3['config']
        )
        self.client = boto3.client(
            's3',
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            region_name=region,
            verify=verify,
            config=self.config
        )

    @staticmethod
    def __bucket_name_and_prefix(path):
        path_tuple = path.split('/', 1)
        if len(path_tuple) == 2:
            bucket_name, prefix = path_tuple
        else:
            bucket_name = path
            prefix = ''
        return bucket_name, prefix

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        nodes = list()
        if node and (
                path := node.get('path', node['name'])
        ):
            bucket_name, prefix = self.__bucket_name_and_prefix(path)
            if 'next_marker' in node:
                resp = self.client.list_objects(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    Delimiter='/',
                    Marker=node['next_marker']
                )
            else:
                resp = self.client.list_objects(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    Delimiter='/'
                )
            if 'CommonPrefixes' in resp:
                for item in resp['CommonPrefixes']:
                    node = dict()
                    node['type'] = 'dir'
                    node['name'] = item['Prefix'].replace(prefix, '')
                    node['path'] = '/'.join([bucket_name, item['Prefix']])
                    node['key'] = item['Prefix']
                    node['children'] = []
                    nodes.append(node)
            if 'Contents' in resp:
                for item in resp['Contents']:
                    if not item['Key'].endswith('/'):
                        node = dict()
                        node['type'] = 'file'
                        node['name'] = os.path.basename(item['Key'])
                        node['path'] = '/'.join([bucket_name, item['Key']])
                        node['etag'] = item['ETag']
                        node['key'] = item['Key']
                        node['last_write_time'] = item['LastModified'].timestamp()
                        node['size'] = item['Size']
                        node['storage_class'] = item['StorageClass']
                        node['display_name'] = item.get('Owner', {}).get('DisplayName', '')
                        node['owner_id'] = item.get('Owner', {}).get('ID', '')
                        nodes.append(node)
            if resp['IsTruncated']:
                node = dict()
                node['type'] = 'file'
                node['name'] = '...'
                node['path'] = path
                node['next_marker'] = resp['NextMarker']
                node['sibling'] = []
                nodes.append(node)
        else:
            resp = self.client.list_buckets()
            for bucket in resp['Buckets']:
                node = dict()
                node['type'] = 'dir'
                node['creation_date'] = bucket['CreationDate'].isoformat()
                node['name'] = bucket['Name']
                node['path'] = bucket['Name']
                node['children'] = []
                try:
                    loc_resp = self.client.get_bucket_location(Bucket=bucket['Name'])
                except Exception:
                    node['location'] = ''
                else:
                    node['location'] = loc_resp['LocationConstraint'] or ''
                nodes.append(node)
        return nodes

    @get_nodes.register(str)
    def _(self, path):
        nodes = list()
        if path:
            bucket_name, prefix = self.__bucket_name_and_prefix(path)
            resp = self.client.list_objects(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            if 'CommonPrefixes' in resp:
                for item in resp['CommonPrefixes']:
                    node = dict()
                    node['type'] = 'dir'
                    node['name'] = item['Prefix'].replace(prefix, '')
                    node['path'] = '/'.join([bucket_name, item['Prefix']])
                    node['key'] = item['Prefix']
                    node['children'] = []
                    nodes.append(node)
            if 'Contents' in resp:
                for item in resp['Contents']:
                    if not item['Key'].endswith('/'):
                        node = dict()
                        node['type'] = 'file'
                        node['name'] = os.path.basename(item['Key'])
                        node['path'] = '/'.join([bucket_name, item['Key']])
                        node['etag'] = item['ETag']
                        node['key'] = item['Key']
                        node['last_write_time'] = item['LastModified'].timestamp()
                        node['size'] = item['Size']
                        node['storage_class'] = item['StorageClass']
                        node['display_name'] = item.get('Owner', {}).get('DisplayName', '')
                        node['owner_id'] = item.get('Owner', {}).get('ID', '')
                        nodes.append(node)
            if resp['IsTruncated']:
                node = dict()
                node['type'] = 'file'
                node['name'] = '...'
                node['path'] = path
                node['next_marker'] = resp['NextMarker']
                node['sibling'] = []
                nodes.append(node)
        else:
            resp = self.client.list_buckets()
            for bucket in resp['Buckets']:
                node = dict()
                node['type'] = 'dir'
                node['creation_date'] = bucket['CreationDate'].isoformat()
                node['name'] = bucket['Name']
                node['path'] = bucket['Name']
                node['children'] = []
                try:
                    loc_resp = self.client.get_bucket_location(Bucket=bucket['Name'])
                except Exception:
                    node['location'] = ''
                else:
                    node['location'] = loc_resp['LocationConstraint'] or ''
                nodes.append(node)
        return nodes

    @singledispatchmethod
    def get_file(self, *args, **kwargs):
        raise NotImplemented

    @get_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.get_object(Bucket=bucket_name, Key=key)
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        for chunk in resp['Body']:
            f.write(chunk)
        f.seek(0)
        return f

    @get_file.register(str)
    def _(self, path):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.get_object(Bucket=bucket_name, Key=key)
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        for chunk in resp['Body']:
            f.write(chunk)
        f.seek(0)
        return f

    @singledispatchmethod
    def delete_file(self, *args, **kwargs):
        raise NotImplemented

    @delete_file.register(MutableMapping)
    def _(self, node):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.delete_object(Bucket=bucket_name, Key=key)
        return resp

    @delete_file.register(str)
    def _(self, path):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.delete_object(Bucket=bucket_name, Key=key)
        return resp

    @singledispatchmethod
    def put_file(self, *args, **kwargs):
        raise NotImplemented

    @put_file.register(MutableMapping)
    def _(self, node, f):
        path = node.get('path', node['name'])
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.put_object(Bucket=bucket_name, Body=f, Key=key)
        return resp

    @put_file.register(str)
    def _(self, path, f):
        bucket_name, key = self.__bucket_name_and_prefix(path)
        resp = self.client.put_object(Bucket=bucket_name, Body=f, Key=key)
        return resp

    def empty_file(self, node):
        f = io.BytesIO()
        self.put_file(node, f)

    def store_file(self, node, f):
        path = os.path.join(node.get('sepPath', ''), node['name'])
        self.put_file(path, f)

    def __bool__(self):
        return True

    def __del__(self):
        return True


class AWSCloudBatch(AWSCloud):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@weak_cache
class AWSCloudOne(AWSCloud):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


def factory(cloud, ak, sk, endpoint, **kwargs):
    cloud_map = {
        'AliCloud': AliCloudOne,
        'InspurCloud': S3One,
        'KsCloud': S3One,
        'HuaWeiCloud': HuaWeiCloudOne,
        'TencentCloud': TencentCloudOne,
        'S3': S3One,
        'AWS': AWSCloudOne
    }
    client = cloud_map.get(cloud)(ak=ak, sk=sk, endpoint=endpoint, **kwargs)
    return client


class OSSIterator(Iterator):

    def __init__(self, auth, resources={}):
        self.cloud_map = {
            'AliCloud': AliCloudBatch,
            'InspurCloud': S3,
            'KsCloud': S3,
            'HuaWeiCloud': HuaWeiCloudBatch,
            'TencentCloud': TencentCloudBatch,
            'S3': S3,
            'AWS': AWSCloudBatch
        }
        self.auth = auth
        self.client = self.factory(**self.auth)
        self.stack = list()
        if resources:
            self.stack.append(resources)
        self.node = dict()

    def __iter__(self):
        return self

    def __bool__(self):
        return bool(self.stack)

    def __next__(self):
        while len(self.stack) > 0:
            self.node = OSSDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'OSS'
            if 'children' in self.node:
                if self.node['children']:
                    children = self.node['children']
                else:
                    children = self.get_nodes(self.node)
                children.reverse()
                self.stack.extend(children)
            elif 'sibling' in self.node:
                if self.node['sibling']:
                    sibling = self.node['sibling']
                else:
                    sibling = self.get_nodes(self.node)
                sibling.reverse()
                self.stack.extend(sibling)
            else:
                return self.node, partial(self.get_file, self.node)
        raise StopIteration

    def factory(self, cloud, ak, sk, endpoint, **kwargs):
        client = self.cloud_map.get(cloud)(
            ak=ak,
            sk=sk,
            endpoint=endpoint,
            **kwargs
        )
        return client

    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    def add_nodes(self, nodes):
        self.stack.extend(nodes)

    def get_file(self, node):
        f = self.client.get_file(node)
        md5 = hashlib.file_digest(f, 'md5')
        node['md5'] = md5.hexdigest()
        f.seek(0)
        return f


OSSProxy = factory
