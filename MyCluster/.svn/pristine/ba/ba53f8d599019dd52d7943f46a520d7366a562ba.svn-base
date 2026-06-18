# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging.config
from common.globals import Variables
from server.kafkaserver import ScanEngine


if __name__ == '__main__':
    variables = Variables()
    logging.config.dictConfig(variables.logging)
    app = ScanEngine()
    app.run()
