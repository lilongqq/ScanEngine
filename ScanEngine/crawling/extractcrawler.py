#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
import traceback
import hashlib
import threading
import shutil
from tempfile import SpooledTemporaryFile
from pathlib import Path
from collections import UserDict
from functools import partial
from crawling.interface import Iterator
from common.globals import Variables
from .archives import ZIP, TAR, RAR, SevenZip


class TmpdirDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['last_write_time']
        elif item == 'diff':
            return self.data.get('md5')
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return 'md5' in self.data
        else:
            return item in self.data


class Dir(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_nodes(self, dir):
        nodes = list()
        for path in Path(dir['fspath']).iterdir():
            if path.is_file():
                node = self.format_file(path)
                node['path'] = str(Path(dir['path'], node['name']))
                nodes.append(node)
            elif path.is_dir():
                node = self.format_dir(path)
                node['path'] = str(Path(dir['path'], node['name']))
                nodes.append(node)
            else:
                self.logger.info('ignore {}'.format(path))
        return nodes

    def get_file(self, node):
        fdst = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        fspath = node['fspath']
        with open(fspath, 'rb') as fsrc:
            shutil.copyfileobj(fsrc, fdst)
            fdst.seek(0)
            return fdst

    def format_dir(self, path: Path):
        node = dict()
        stat = path.stat()
        node['type'] = 'dir'
        node['name'] = self.decode_name(path.name)
        node['fspath'] = str(path)
        node['size'] = int(stat.st_size)
        node['last_access_time'] = stat.st_atime
        node['last_write_time'] = stat.st_mtime
        node['children'] = []
        return node

    def format_file(self, path: Path):
        node = dict()
        stat = path.stat()
        node['type'] = 'file'
        node['name'] = self.decode_name(path.name)
        node['fspath'] = str(path)
        node['size'] = int(stat.st_size)
        node['last_access_time'] = stat.st_atime
        node['last_write_time'] = stat.st_mtime
        return node

    def decode_name(self, name):
        name_bytes = os.fsencode(name)
        for encoding in ('utf-8', 'gb18030', 'big5hkscs'):
            try:
                name = name_bytes.decode(encoding)
            except UnicodeDecodeError:
                self.logger.info(traceback.format_exc())
            else:
                break
        return name


class TmpdirIterator(Iterator):

    def __init__(self, tmpdir):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.tmpdir = tmpdir
        self.client = Dir()
        self.stack = list()
        node = self.client.format_dir(Path(tmpdir.name))
        node['path'] = ''
        self.stack.append(node)
        self.dir = dict()
        self.node = dict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = self.stack.pop()
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

    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    def get_file(self, node):
        f = self.client.get_file(node)
        md5 = hashlib.file_digest(f, 'md5')
        node['md5'] = md5.hexdigest()
        f.seek(0)
        return f


class ExtractIterator(Iterator):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.local = threading.local()
        self.extract = Variables().extract

    def __call__(self, node, file):
        self._cleanup_iterator()
        self.local.node = node
        if node.get('type', 'file') == 'file' and node['name'].lower().endswith(
            (
                '.zip',
                '.zipx',
                '.tar',
                '.tar.gz',
                '.tgz',
                '.tar.bz2',
                '.tbz2',
                '.tbz',
                '.tar.xz',
                '.txz',
                '.tar.lz',
                '.tar.lzma',
                '.tar.z',
                '.tar.zst',
                '.tzst',
                '.rar',
                '.7z'
            )
        ):
            if node['name'].lower().endswith('.rar'):
                if RAR.is_archive(file):
                    tmpdir = RAR.extractall(file)
                    self.local.file = None
                    self.local.iterator = TmpdirIterator(tmpdir)
                else:
                    self.local.file = file
                    self.local.iterator = None
            elif node['name'].lower().endswith('.7z'):
                if SevenZip.is_archive(file):
                    tmpdir = SevenZip.extractall(file)
                    self.local.file = None
                    self.local.iterator = TmpdirIterator(tmpdir)
                else:
                    self.local.file = file
                    self.local.iterator = None
            elif node['name'].lower().endswith(
                (
                    '.zip',
                    '.zipx'
                )
            ):
                if ZIP.is_archive(file):
                    tmpdir = ZIP.extractall(file)
                    self.local.file = None
                    self.local.iterator = TmpdirIterator(tmpdir)
                else:
                    self.local.file = file
                    self.local.iterator = None
            else:
                if TAR.is_archive(file):
                    tmpdir = TAR.extractall(file)
                    self.local.file = None
                    self.local.iterator = TmpdirIterator(tmpdir)
                else:
                    self.local.file = file
                    self.local.iterator = None
        else:
            self.local.file = file
            self.local.iterator = None

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.local.iterator:
                try:
                    node, stream = next(self.local.iterator)
                except StopIteration:
                    self._cleanup_iterator()
                    raise
                file = stream()
                node['auth'] = self.local.node.get('auth', None)
                node['cls'] = self.local.node['cls']
                node['path'] = '#'.join(
                    [
                        self.local.node['path'],
                        node['path']
                    ]
                )
                return TmpdirDict(node), file
            elif self.local.file:
                node = self.local.node
                file = self.local.file
                self.local.node = None
                self.local.file = None
                return node, file
            else:
                raise StopIteration

    def _cleanup_iterator(self):
        iterator = getattr(self.local, 'iterator', None)
        if iterator:
            tmpdir = getattr(iterator, 'tmpdir', None)
            if tmpdir:
                try:
                    tmpdir.cleanup()
                except Exception:
                    self.logger.error(traceback.format_exc())
            self.local.iterator = None

    def __bool__(self):
        return self.extract
