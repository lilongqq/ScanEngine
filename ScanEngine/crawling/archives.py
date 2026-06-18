#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import tempfile
import tarfile
import zipfile
import py7zr
import rarfile
from ex3rd.rarfile import RarFile


class ZIP(object):

    @staticmethod
    def is_archive(file):
        result = zipfile.is_zipfile(file)
        file.seek(0)
        return result

    @staticmethod
    def extractall(file):
        tmpdir = tempfile.TemporaryDirectory()
        with zipfile.ZipFile(file) as myzip:
            members = myzip.infolist()
            for member in members:
                if not member.flag_bits & 1 << 11:
                    member.filename = (
                        member.filename
                        .encode('cp437')
                        .decode(
                            encoding='utf-8',
                            errors='surrogateescape'
                        )
                    )
            myzip.extractall(tmpdir.name, members=members)
        return tmpdir


class TAR(object):

    @staticmethod
    def is_archive(file):
        result = tarfile.is_tarfile(file)
        file.seek(0)
        return result

    @staticmethod
    def extractall(file):
        tmpdir = tempfile.TemporaryDirectory()
        with tarfile.open(fileobj=file) as mytar:
            mytar.extractall(tmpdir.name)
        return tmpdir


class RAR(object):

    @staticmethod
    def is_archive(file):
        result = rarfile.is_rarfile(file)
        file.seek(0)
        return result

    @staticmethod
    def extractall(file):
        tmpdir = tempfile.TemporaryDirectory()
        with RarFile(file) as myrar:
            myrar.extractall(tmpdir.name)
        return tmpdir


class SevenZip(object):

    @staticmethod
    def is_archive(file):
        result = py7zr.is_7zfile(file)
        file.seek(0)
        return result

    @staticmethod
    def extractall(file):
        tmpdir = tempfile.TemporaryDirectory()
        with py7zr.SevenZipFile(file) as myzip:
            myzip.extractall(tmpdir.name)
        return tmpdir
