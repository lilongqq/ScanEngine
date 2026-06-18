# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import rarfile


class RarFile(rarfile.RarFile):

    def extractall(self, path=None, members=None, pwd=None):
        if super().needs_password():
            raise rarfile.PasswordRequired('File requires password')
        super().extractall(path, members, pwd)
