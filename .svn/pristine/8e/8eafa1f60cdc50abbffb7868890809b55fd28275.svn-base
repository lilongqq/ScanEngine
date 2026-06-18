import logging
import os
import threading
import time
import traceback

from Crypto.Cipher import ARC4
from common.globals import Variables

class DatabaseFinger(object):
    lock = threading.RLock()

    def __init__(self, identity):
        self.cache_sql = []
        self.encrypt = ARC4.new(b'Spinfo')
        self.model_path =  os.path.join(Variables().strategy, identity)
        self.model_file = os.path.join(self.model_path, 'database_fingerprint.mod')
        if not os.path.isdir(self.model_path):
            os.makedirs(self.model_path)
        self._logger = logging.getLogger(self.__class__.__name__)


    def fingerprint(self, *args):
        identity, node, data = args
        with self.lock:
            with open(self.model_file, 'ab') as f:
                f.write(self.encrypt.encrypt(data+b'\r\n'))


# d = DatabaseFinger('.', 'job_id')
# d.fingerprint('./', b'data')