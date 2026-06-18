#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from ftplib import FTP, FTP_TLS


class FTPS(FTP_TLS):

    def ntransfercmd(self, cmd, rest=None):
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        if getattr(self, '_prot_p'):
            conn = self.context.wrap_socket(
                conn,
                server_hostname=self.host,
                session=self.sock.session
            )
        return conn, size
