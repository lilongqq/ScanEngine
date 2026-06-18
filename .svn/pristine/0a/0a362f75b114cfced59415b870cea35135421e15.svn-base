#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import logging
import hashlib
import re
import os.path
from collections.abc import MutableMapping
from collections import UserDict
from functools import singledispatchmethod, partial
from subprocess import CalledProcessError
from paramiko import Transport, SFTPClient, AuthenticationException, SSHClient
from paramiko.rsakey import RSAKey
from paramiko.dsskey import DSSKey
from paramiko.ecdsakey import ECDSAKey
from paramiko.ed25519key import Ed25519Key
from common.functools import gen_key
from common.conntool import thread_local, alive_check, weak_cache
from crawling.interface import Iterator, Client


class CoreMailDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['last_write_time']
        elif item == 'diff':
            return gen_key(
                mid=self.data.get('mid'),
                last_write_time=self.data.get('last_write_time')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return {'last_write_time', 'mid'}.issubset(self.data)
        else:
            return item in self.data


class CoreMail(Client):

    size_pattern = re.compile(
        r'''
        (?:(?P<M>\d+)M)?
        (?:(?P<K>\d+)K)?
        (?:(?P<B>\d+)B)?
        ''',
        re.VERBOSE | re.IGNORECASE
    )

    def __init__(
        self,
        userutil,
        host,
        port,
        username,
        password=None,
        pkey=None,
        keyt='RSA',
        encoding='utf-8',
        **kwargs
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.userutil = userutil
        self.host = host
        self.port = int(port)
        self.sock = (self.host, self.port)
        self.username = username
        self.password = password
        self.keyt = keyt
        self.encoding = encoding
        if pkey:
            if self.keyt == 'RSA':
                self.pkey = RSAKey.from_private_key(
                    io.StringIO(pkey),
                    password=self.password
                )
            elif self.keyt == 'DSS':
                self.pkey = DSSKey.from_private_key(
                    io.StringIO(pkey),
                    password=self.password
                )
            elif self.keyt == 'ECDSA':
                self.pkey = ECDSAKey.from_private_key(
                    io.StringIO(pkey),
                    password=self.password
                )
            elif self.keyt == 'Ed25519':
                self.pkey = Ed25519Key.from_private_key(
                    io.StringIO(pkey),
                    password=self.password
                )
            else:
                raise ValueError(self.keyt)
            try:
                self.transport = Transport(
                    self.sock,
                    disabled_algorithms={
                        'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']
                    }
                )
                self.transport.connect(
                    username=self.username,
                    pkey=self.pkey
                )
            except AuthenticationException:
                self.transport = Transport(self.sock)
                self.transport.connect(
                    username=self.username,
                    pkey=self.pkey
                )
        else:
            self.transport = Transport(self.sock)
            self.transport.connect(
                username=self.username,
                password=self.password
            )
        self.transport.set_keepalive(30)
        self.transport.sock.settimeout(600)

    def select_user(self):
        channel = self.transport.open_session()
        try:
            channel.exec_command(f'{self.userutil} --select-user @')
            stdout = channel.makefile("r", 256*1024)
            stderr = channel.makefile_stderr("r", 4*1024)
            out = stdout.readlines()
            err = stderr.read()
            returncode = channel.recv_exit_status()
            if returncode:
                raise CalledProcessError(
                    returncode,
                    cmd=f'{self.userutil} --select-user @',
                    stderr=err
                )
            return self.format_userlist(out)
        finally:
            channel.close()

    @staticmethod
    def format_userlist(userlist):
        nodes = list()
        for user in userlist:
            if user:
                node = dict()
                node['type'] = 'user'
                node['name'] = user.strip()
                node['path'] = node['name']
                node['smtp_address'] = node['name']
                node['children'] = []
                nodes.append(node)
        return nodes

    def list_msg(self, user):
        channel = self.transport.open_session()
        try:
            channel.exec_command(f'{self.userutil} --list-msg {user}')
            stdout = channel.makefile("r", 256*1024)
            stderr = channel.makefile_stderr("r", 4*1024)
            out = stdout.readlines()
            err = stderr.read()
            returncode = channel.recv_exit_status()
            if returncode:
                raise CalledProcessError(
                    returncode,
                    cmd=f'{self.userutil} --list-msg {user}',
                    stderr=err
                )
            return self.format_msgs(user, out)
        finally:
            channel.close()

    def format_msgs(self, user, msgs):
        nodes = list()
        for line in msgs:
            if line:
                if line.startswith((user, '---')):
                    self.logger.info(line)
                else:
                    node = dict()
                    (
                        fid,
                        mid,
                        flags,
                        size_str,
                        hex_time,
                        direction,
                        dec_time,
                        sender,
                        subject_offset
                    ) = line.split(maxsplit=8)
                    subs = subject_offset.rsplit(maxsplit=1)
                    if len(subs) > 1:
                        (
                            subject,
                            offset
                        ) = subs
                    else:
                        subject = ''
                        offset = subs
                    node = dict()
                    node['type'] = 'file'
                    node['fid'] = fid
                    node['mid'] = mid
                    node['flags'] = flags
                    node['size_str'] = size_str
                    node['hex_time'] = hex_time
                    node['direction'] = direction
                    node['dec_time'] = dec_time
                    node['sender'] = sender
                    node['subject'] = subject
                    node['offset'] = offset
                    node['name'] = f"{'-'.join([str(subject), mid])}.eml"
                    node['path'] = os.path.join(
                        user,
                        node['name']
                    )
                    node['size'] = self.format_size(size_str)
                    node['last_write_time'] = int(hex_time, 16)
                    node['smtp_address'] = user
                    nodes.append(node)
        return nodes

    @classmethod
    def format_size(cls, size_str):
        count = 0
        match = cls.size_pattern.match(size_str)
        if match:
            if m := match.group('M'):
                count += int(m) * 1024 * 1024
            if k := match.group('K'):
                count += int(k) * 1024
            if b := match.group('B'):
                count += int(b)
        return count

    def dump_msg(self, user, mid):
        file = io.BytesIO()
        channel = self.transport.open_session()
        try:
            channel.exec_command(f'{self.userutil} --dump-msg {user} {mid}')
            stdout = channel.makefile("rb", 256*1024)
            stderr = channel.makefile_stderr("r", 4*1024)
            while True:
                data = stdout.read(256*1024)
                if not data:
                    break
                file.write(data)
            err = stderr.read()
            returncode = channel.recv_exit_status()
            if returncode:
                raise CalledProcessError(
                    returncode,
                    cmd=f'{self.userutil} --dump-msg {user} {mid}',
                    stderr=err
                )
            file.seek(0)
            return file
        finally:
            channel.close()

    def get_nodes(self, node: dict):
        if smtp_address := node.get('smtp_address'):
            return self.list_msg(smtp_address)
        else:
            return self.select_user()

    def get_file(self, node):
        user = node['smtp_address']
        mid = node['mid']
        return self.dump_msg(user, mid)

    def __bool__(self):
        return self.transport.is_active()

    def __del__(self):
        self.transport.close()


@weak_cache
@alive_check
class CoreMailOne(CoreMail):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@thread_local
class CoreMailBatch(CoreMail):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class CoreMailIterator(Iterator):

    def __init__(self, auth, resources={}):
        self.auth = auth
        self.client = CoreMailBatch(**self.auth)
        self.stack = list()
        if resources:
            self.stack.append(resources)
        self.node = CoreMailDict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = CoreMailDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'COREMAIL'
            if 'children' in self.node:
                if self.node['children']:
                    children = self.node['children']
                else:
                    children = self.get_nodes(self.node)
                children.reverse()
                self.stack.extend(children)
            else:
                return self.node, partial(self.get_file, self.node)
        raise StopIteration

    def __bool__(self):
        return bool(self.stack)

    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    def add_nodes(self, nodes):
        self.stack.extend(nodes)

    def get_file(self, node):
        f = self.client.get_file(node)
        md5 = hashlib.md5()
        md5.update(f.getbuffer())
        node['md5'] = md5.hexdigest()
        return f
