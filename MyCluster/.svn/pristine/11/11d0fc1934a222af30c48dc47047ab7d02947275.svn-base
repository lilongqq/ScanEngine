#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from ssl import PROTOCOL_TLSv1
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager


class NoVerifyHTTPAdapterTLSv1(HTTPAdapter):

    def cert_verify(self, conn, url, verify, cert):
        super().cert_verify(
            conn=conn,
            url=url,
            verify=False,  # the privite server usually can't be trusted
            cert=cert)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            ssl_version=PROTOCOL_TLSv1,  # must inclued this in debian
            block=block,
            strict=True,
            **pool_kwargs
        )
