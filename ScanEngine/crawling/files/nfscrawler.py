# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
import os.path
import socket
from tempfile import SpooledTemporaryFile
import hashlib
from selectors import DefaultSelector, EVENT_READ
from collections import UserDict
from collections.abc import MutableMapping
from functools import singledispatchmethod, partial
from pyNfsClient import Portmap, Mount, NFSv3, MNT3_OK, NFS_PROGRAM, NFS_V3, NFS3_OK, NF3DIR, NF3REG
from crawling.interface import Iterator, Client
from common.functools import gen_key
from common.conntool import client_pool, weak_cache, thread_local


class NFSError(Exception):

    def __init__(self, res):
        self.res = res

    def __str__(self):
        return str(self.res)


class NFSDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return self.data['last_write_time']
        elif item == 'diff':
            return gen_key(
                handle=self.data.get('handle'),
                last_write_time=self.data.get('last_write_time')
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'last_write_time' in self.data
        elif item == 'diff':
            return {'last_write_time', 'handle'}.issubset(self.data)
        else:
            return item in self.data


class NFS(Client):

    def __init__(self, host, mnt_port=0, nfs_port=0, timeout=600, encoding='utf-8'):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.host = host
        self.timeout = int(timeout)
        self.encoding = encoding
        self.auth = {
            "flavor": 1,
            "machine_name": socket.gethostname(),
            "uid": os.getuid(),
            "gid": os.getgid(),
            "aux_gid": os.getgroups()
        }
        self.mnt_port = int(mnt_port)
        self.nfs_port = int(nfs_port)
        if not (self.mnt_port and self.nfs_port):
            self.portmap = Portmap(self.host, timeout=self.timeout)
            self.portmap.connect()
            if not self.mnt_port:
                self.mnt_port = self.portmap.getport(
                    Mount.program,
                    Mount.program_version
                )
            if not self.nfs_port:
                self.nfs_port = self.portmap.getport(NFS_PROGRAM, NFS_V3)
        self.mount = Mount(
            host=self.host,
            port=self.mnt_port,
            timeout=self.timeout,
            auth=self.auth
        )
        self.mount.connect()
        self.nfsv3 = NFSv3(
            host=self.host,
            port=self.nfs_port,
            timeout=self.timeout,
            auth=self.auth
        )
        self.nfsv3.connect()
        self.selector = DefaultSelector()
        self.selector.register(self.mount.client, EVENT_READ)
        self.selector.register(self.nfsv3.client, EVENT_READ)

    @singledispatchmethod
    def get_nodes(self, *args, **kwargs):
        raise NotImplemented

    @get_nodes.register(MutableMapping)
    def _(self, node):
        if node and node.get('path', node['name']):
            path = node.get('path', node['name'])
        else:
            path = ''
        if node.get('handle', None):
            dir_handle = node['handle']
            nodes = self.readdirplus(
                bytes.fromhex(dir_handle)
            )
            for node in nodes:
                node['path'] = os.path.join(path, node['name'])
                node['dir_handle'] = dir_handle
        elif path:
            mount3res = self.mount.mnt(path)
            if mount3res['status'] == MNT3_OK:
                dir_handle = mount3res['mountinfo']['fhandle'].hex()
                nodes = self.readdirplus(mount3res['mountinfo']['fhandle'])
                for node in nodes:
                    node['path'] = os.path.join(path, node['name'])
                    node['dir_handle'] = dir_handle
            else:
                raise OSError
        else:
            nodes = self.exports()
        return nodes

    @get_nodes.register(str)
    def _(self, path):
        nodes = list()
        if path:
            mount3res = self.mount.mnt(path)
            if mount3res['status'] == MNT3_OK:
                handle = mount3res['mountinfo']['fhandle']
                getattr3res = self.nfsv3.getattr(handle)
                if getattr3res['status'] == NFS3_OK:
                    attr = getattr3res['attributes']
                    if attr['type'] in (NF3REG, NF3DIR):
                        node = dict()
                        node['name'] = os.path.basename(path)
                        node['path'] = path
                        node['handle'] = handle.hex()
                        node['mode'] = attr['mode']
                        node['nlink'] = attr['nlink']
                        node['uid'] = attr['uid']
                        node['gid'] = attr['gid']
                        node['size'] = attr['size']
                        node['used'] = attr['used']
                        node['fsid'] = attr['fsid']
                        node['fileid'] = attr['fileid']
                        node['create_time'] = attr['ctime']['seconds']
                        node['last_access_time'] = attr['atime']['seconds']
                        node['last_write_time'] = attr['mtime']['seconds']
                        if attr['type'] == NF3DIR:
                            node['children'] = []
                        nodes.append(node)
                else:
                    raise OSError
        else:
            nodes = self.exports()
        return nodes

    def get_file(self, node):
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        fh = bytes.fromhex(node['handle'])
        offset = 0
        stale_retried = False
        while True:
            read3res = self.nfsv3.read(fh, offset=offset)
            if read3res['status'] == NFS3_OK:
                f.write(read3res['resok']['data'])
                offset += read3res['resok']['count']
                if read3res['resok']['eof']:
                    break
            elif read3res['status'] == 70 and not stale_retried and node.get('path'):
                # NFS3ERR_STALE: handle expired, re-resolve from path
                path = node['path']
                dir_fh = self._get_dir_handle(os.path.dirname(path))
                lookup3res = self.nfsv3.lookup(dir_fh, os.path.basename(path))
                if lookup3res['status'] != NFS3_OK:
                    raise NFSError(lookup3res)
                fh = lookup3res['resok']['object']['data']
                f.seek(0)
                f.truncate(0)
                offset = 0
                stale_retried = True
            else:
                raise NFSError(read3res)
        f.seek(0)
        return f

    def _get_dir_handle(self, path):
        exports = self.mount.export()
        export_paths = [
            item.ex_dir.decode(self.encoding, errors='surrogateescape')
            for item in exports
        ]
        # 找最长匹配的 export 根路径
        match = None
        for ep in sorted(export_paths, key=len, reverse=True):
            if path == ep or path.startswith(ep.rstrip('/') + '/'):
                match = ep
                break
        if match is None:
            raise NFSError({'status': 2, 'mountinfo': None})
        mount3res = self.mount.mnt(match)
        if mount3res['status'] != MNT3_OK:
            raise NFSError(mount3res)
        fh = mount3res['mountinfo']['fhandle']
        # 逐级 LOOKUP 到目标目录
        rel = os.path.relpath(path, match)
        if rel != '.':
            for part in rel.split(os.sep):
                lookup3res = self.nfsv3.lookup(fh, part)
                if lookup3res['status'] != NFS3_OK:
                    raise NFSError(lookup3res)
                fh = lookup3res['resok']['object']['data']
        return fh

    def store_file(self, node, f):
        self.logger.info('store_file node:')
        self.logger.info(node)
        sep_path = node.get('sepPath', '')
        file_name = node.get('name', '')
        if not sep_path or not file_name:
            raise ValueError('sepPath or name is empty')
        dir_fh = self._get_dir_handle(sep_path)
        create3res = self.nfsv3.create(dir_fh, file_name, 0)
        if create3res['status'] != NFS3_OK:
            raise NFSError(create3res)
        obj = create3res['resok']['obj']
        if obj.get('present'):
            file_fh = obj['handle']['data']
        else:
            lookup3res = self.nfsv3.lookup(dir_fh, file_name)
            if lookup3res['status'] != NFS3_OK:
                raise NFSError(lookup3res)
            file_fh = lookup3res['resok']['object']['data']
        offset = 0
        while True:
            chunk = f.read(32768)
            if not chunk:
                break
            write3res = self._write_binary(file_fh, offset, chunk)
            if write3res['status'] != NFS3_OK:
                raise NFSError(write3res)
            offset += write3res['resok']['count']

    def _write_binary(self, file_fh, offset, data_bytes, stable_how=2):
        from pyNfsClient.pack import nfs_pro_v3Packer, nfs_pro_v3Unpacker
        from pyNfsClient.rtypes import write3args, nfs_fh3
        from pyNfsClient.const import NFS3_PROCEDURE_WRITE
        packer = nfs_pro_v3Packer()
        packer.pack_write3args(write3args(
            file=nfs_fh3(file_fh),
            offset=offset,
            count=len(data_bytes),
            stable=stable_how,
            data=data_bytes
        ))
        res = self.nfsv3.nfs_request(NFS3_PROCEDURE_WRITE, packer.get_buffer(), self.nfsv3.auth)
        unpacker = nfs_pro_v3Unpacker(res)
        return unpacker.unpack_write3res()

    def empty_file(self, node):
        path = node.get('path', node['name'])
        if not path:
            raise ValueError('path is empty')
        file_name = node.get('name', os.path.basename(path))
        dir_path = os.path.dirname(path)
        if node.get('dir_handle'):
            dir_fh = bytes.fromhex(node['dir_handle'])
        else:
            dir_fh = self._get_dir_handle(dir_path)
        self.nfsv3.remove(dir_fh, file_name)
        create3res = self.nfsv3.create(dir_fh, file_name, 0)
        if create3res['status'] != NFS3_OK:
            raise NFSError(create3res)

    @singledispatchmethod
    def delete_file(self, *args, **kwargs):
        raise NotImplemented

    @delete_file.register(MutableMapping)
    def _(self, node):
        if node.get('dir_handle'):
            dir_fh = bytes.fromhex(node['dir_handle'])
        else:
            dir_fh = self._get_dir_handle(os.path.dirname(node['path']))
        remove3res = self.nfsv3.remove(dir_fh, node['name'])
        if remove3res['status'] != NFS3_OK:
            raise NFSError(remove3res)

    @delete_file.register(str)
    def _(self, path):
        dir_fh = self._get_dir_handle(os.path.dirname(path))
        remove3res = self.nfsv3.remove(dir_fh, os.path.basename(path))
        if remove3res['status'] != NFS3_OK:
            raise NFSError(remove3res)

    def exports(self):
        nodes = list()
        exports = self.mount.export()
        for item in exports:
            node = dict()
            dirpath = item.ex_dir.decode(
                self.encoding,
                errors='surrogateescape'
            )
            node['type'] = 'dir'
            node['name'] = dirpath
            node['path'] = dirpath
            node['children'] = []
            nodes.append(node)
        return nodes

    def readdirplus(self, fh3, cookie=0, cookie_verf='0'):
        nodes = list()
        while True:
            readdir3res = self.nfsv3.readdirplus(fh3, cookie, cookie_verf)
            if readdir3res['status'] == NFS3_OK:
                entries = readdir3res['resok']['reply']['entries']
                for item in entries:
                    node = dict()
                    node['name'] = item['name'].decode(
                        self.encoding,
                        errors='surrogateescape'
                    )
                    if node['name'] == '.' or node['name'] == '..':
                        continue
                    handle = item['name_handle']['handle']['data']
                    node['handle'] = handle.hex()
                    name_attributes = item['name_attributes']
                    if name_attributes['present']:
                        attr = name_attributes['attributes']
                        if attr['type'] not in (NF3REG, NF3DIR):
                            continue
                        node['mode'] = attr['mode']
                        node['nlink'] = attr['nlink']
                        node['uid'] = attr['uid']
                        node['gid'] = attr['gid']
                        node['size'] = attr['size']
                        node['used'] = attr['used']
                        node['fsid'] = attr['fsid']
                        node['fileid'] = attr['fileid']
                        node['create_time'] = attr['ctime']['seconds']
                        node['last_access_time'] = attr['atime']['seconds']
                        node['last_write_time'] = attr['mtime']['seconds']
                        if attr['type'] == NF3DIR:
                            node['type'] = 'dir'
                            node['children'] = []
                        else:
                            node['type'] = 'file'
                        nodes.append(node)
                else:
                    eof = readdir3res['resok']['reply']['eof']
                    if eof:
                        break
                    cookie = item['cookie']
                    cookie_verf = readdir3res['resok']['cookieverf']
            else:
                raise NFSError(readdir3res)
        return nodes

    def __bool__(self):
        if getattr(self, 'selector', False):
            events = self.selector.select(timeout=0)
            return not events
        else:
            return False

    def __del__(self):
        if getattr(self, 'nfsv3', False):
            self.nfsv3.disconnect()
        if getattr(self, 'mount', False):
            if getattr(self.mount, 'path', None):
                self.mount.umnt(self.auth)
            self.mount.disconnect()
        if getattr(self, 'portmap', False):
            self.portmap.disconnect()
        if getattr(self, 'selector', False):
            self.selector.close()


@thread_local
class NFSBatch(NFS):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


@weak_cache
@client_pool(max_connections=3)
class NFSOne(NFS):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)


class NFSIterator(Iterator):

    def __init__(self, auth, resources={}):
        self.auth = auth
        self.client = NFSBatch(**self.auth)
        self.stack = list()
        if resources:
            self.stack.append(resources)
        self.node = NFSDict()

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            self.node = NFSDict(self.stack.pop())
            self.node['auth'] = self.auth
            self.node['cls'] = 'NFS'
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
        md5 = hashlib.file_digest(f, 'md5')
        node['md5'] = md5.hexdigest()
        f.seek(0)
        return f
