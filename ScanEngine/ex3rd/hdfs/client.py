#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from hdfs import Client
from requests import Session


class SecureClient(Client):

    def __init__(self, url, user='root', cert=None, verify=True, **kwargs):
        session = Session()
        session.params['user.name'] = user
        if verify:
            if cert:
                if ',' in cert:
                    session.cert = [path.strip() for path in cert.split(',')]
                else:
                    session.cert = cert
        else:
            session.verify = verify
        super(SecureClient, self).__init__(url, session=session, **kwargs)
