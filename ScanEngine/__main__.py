# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging.config
from common.globals import Variables
from server.restserver import ScanEngine

if __name__ == '__main__':
    variables = Variables()
    logging.config.dictConfig(variables.logging)
    logging.getLogger('pyNfsClient').setLevel(logging.WARNING)
    logging.getLogger('SMB.SMBConnection').setLevel(logging.WARNING)
    app = ScanEngine()
    app.run()
