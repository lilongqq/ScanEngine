#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from hbase.scan import Scan


class MyScan(Scan):

    def create_scanner(self, tbl_name, scanner_payload='<Scanner batch="1000"/>'):
        if self.scanner is None:
            scan_url = self._get_scanner(tbl_name, scanner_payload)
            return scan_url
        else:
            return self.scanner
