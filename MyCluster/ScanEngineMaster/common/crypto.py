# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding


class Asymmetric(object):

    def __init__(self, pkey):
        self.logger = logging.getLogger(self.__class__.__name__)
        with open(pkey, 'rb') as f:
            data = f.read()
            self.private_key = serialization.load_pem_private_key(
                data,
                password=None
            )
            self.public_key = self.private_key.public_key()

    def decrypt(self, ciphertext):
        cipherdata = base64.decodebytes(ciphertext.encode('ascii'))
        plaindata = self.private_key.decrypt(
            ciphertext=cipherdata,
            padding=padding.PKCS1v15()
        )
        plaintext = plaindata.decode('utf-8')
        return plaintext

    def encrypt(self, plaintext):
        cipherdata = self.public_key.encrypt(
            plaintext=plaintext.encode('utf-8'),
            padding=padding.PKCS1v15()
        )
        ciphertext = base64.encodebytes(cipherdata).decode('ascii')
        return ciphertext
