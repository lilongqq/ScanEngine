#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import paramiko

class Transport(paramiko.Transport):

    def __del__(self):
        self.close()
