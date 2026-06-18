#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import io
import os
import traceback
import hashlib
import threading
import shutil
from tempfile import SpooledTemporaryFile
from pathlib import Path
from collections.abc import MutableMapping
from collections import UserDict, ChainMap
from functools import singledispatchmethod, partial
from common.functools import gen_key
from crawling.interface import Iterator
from common.globals import Variables
from . import must_combined_suffixes, combined_suffixes_map
from ..archives import ZIP, TAR, RAR, SevenZip


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
        self.adf_files = list()
        self.isolated_files = list()
        self.subfiles = list()
        self.subdirs = list()
        self.combined_files = dict()
        self.value_suffixes_map = ChainMap(*combined_suffixes_map.values())

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self.isolated_files:
                self.node = self.isolated_files.pop()
                self.logger.debug(self.node)
                return self.node, partial(self.get_file, self.node)
            elif self.subfiles:
                for node in self.subfiles:
                    if node['name'].lower() == 'tileset.json':
                        for key, value in self.combined_files.items():
                            if value['suffixes'][0] == '.b3dm':
                                value['size'] += node['size']
                                value['suffixes'].append(node['suffix'])
                                value['children'].append(node)
                                self.logger.info(
                                    'b3dm tileset.json combined_files: {}'.format(
                                        key)
                                )
                                break
                    else:
                        for key, value in self.combined_files.items():
                            if key in node['name'].lower():
                                if node['name'].lower().removeprefix(key).isascii():
                                    value['size'] += node['size']
                                    value['suffixes'].append(node['suffix'])
                                    value['children'].append(node)
                                    break
                self.subfiles.clear()
            elif self.combined_files:
                key, value = self.combined_files.popitem()
                self.logger.info('combined_files: {}'.format(key))
                if set(value['suffixes']).issuperset(must_combined_suffixes[value['suffixes'][0]]):
                    self.node = value
                    self.logger.debug(self.node)
                    return self.node, partial(self.get_entirety, self.node)
                else:
                    self.logger.info(
                        'check integrity failed ignore combined_files: {}'.format(
                            value
                        )
                    )
            elif self.subdirs:
                if self.adf_files:
                    self.handle_adf_files()
                else:
                    for node in self.subdirs:
                        if node['name'].lower() == 'info':
                            break
                    else:
                        self.stack.extend(self.subdirs)
                        self.subdirs.clear()
                        continue
                    self.handle_adf_files()
            elif self.adf_files:
                self.node = self.dir
                self.node['type'] = 'entirety'
                size = 0
                for node in self.adf_files:
                    self.node['children'].append(node)
                    size += node['size']
                else:
                    self.node['size'] = size
                    self.node['name'] = 'coverage.adf'
                    self.adf_files.clear()
                self.logger.debug(self.node)
                return self.node, partial(self.get_entirety, self.node)
            elif self.stack:
                self.dir = self.stack.pop()
                if self.dir['children']:
                    children = self.dir['children']
                else:
                    children = self.get_nodes(self.dir)
                self.dir['children'] = []
                if self.dir['name'].lower().endswith('.gdb'):
                    self.node = self.dir
                    self.node['type'] = 'entirety'
                    self.node['children'] = children
                    return self.node, partial(self.get_entirety, self.node)
                else:
                    for node in children:
                        if node.get('type', 'dir') == 'dir':
                            self.subdirs.append(node)
                        else:
                            if self.handle_combined_key_file(node):
                                continue
                            if self.handle_combined_file(node):
                                continue
                            self.isolated_files.append(node)
            else:
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

    def get_entirety(self, node):
        files = list()
        md5 = hashlib.md5()
        for item in node['children']:
            f = self.get_file(item)
            files.append(f)
            md5.update(bytes.fromhex(item['md5']))
        node['md5'] = md5.hexdigest()
        return files

    def handle_combined_key_file(self, node):
        for suffix in combined_suffixes_map.keys():
            if node['name'].lower().endswith(suffix):
                node['suffix'] = suffix
                combined_dir = dict(self.dir)
                combined_dir['type'] = 'entirety'
                combined_dir['name'] = node['name']
                combined_dir['suffixes'] = [suffix]
                combined_dir['children'] = [node]
                combined_dir['size'] = node['size']
                self.combined_files[
                    node['name'].lower().removesuffix(suffix)
                ] = combined_dir
                return True
        else:
            return False

    def handle_combined_file(self, node):
        for suffix in self.value_suffixes_map.keys():
            if node['name'].lower().endswith(suffix):
                node['suffix'] = suffix
                self.subfiles.append(node)
                return True
        else:
            return False

    def handle_adf_files(self):
        while self.subdirs:
            node = self.subdirs.pop()
            if node['children']:
                children = node['children']
            else:
                children = self.get_nodes(node)
            node['children'] = []

            for item in children:
                if item['name'].lower().endswith(('.adf', '.dat')):
                    self.adf_files.extend(children)
                    break
            else:
                node['children'] = children
                self.stack.append(node)


class ExtractIterator(Iterator):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.local = threading.local()
        self.extract = Variables().extract

    def __call__(self, node, file):
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
                node, stream = next(self.local.iterator)
                file = stream()
                node['path'] = '#'.join(
                    [
                        self.local.node['path'],
                        node['path']
                    ]
                )
                for child in node.get('children', []):
                    child['path'] = '#'.join(
                        [
                            self.local.node['path'],
                            child['path']
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

    def __bool__(self):
        return self.extract
