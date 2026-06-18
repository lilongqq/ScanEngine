# coding=utf-8
#!/usr/bin/env python

import base64

try:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad, unpad
except ImportError:
    print('please install pycryptodome')


class Aes(object):
    def __init__(self):
        self.key = 'AES0123abcmiIO02'.encode('utf-8')
        self.iv = 'fedcba9876543210'.encode('utf-8')
        self.mode = AES.MODE_CBC

    def encode(self, data):
        cipher = AES.new(self.key, self.mode, self.iv)
        pad_pkcs7 = pad(data.encode('utf-8'), AES.block_size, style='pkcs7')
        result = base64.encodebytes(cipher.encrypt(pad_pkcs7))
        encrypted_text = str(result, encoding='utf-8').replace('\n', '')
        return encrypted_text

    def decode(self, data):
        cipher = AES.new(self.key, self.mode, self.iv)
        base64_decrypted = base64.decodebytes(data.encode('utf-8'))
        una_pkcs7 = unpad(cipher.decrypt(base64_decrypted), AES.block_size, style='pkcs7')
        decrypted_text = str(una_pkcs7, encoding='utf-8')
        return decrypted_text


if __name__ == '__main__':
    a = Aes()
    assert a.encode('root') == 'brMsXW2KwFXaCRO/ZXThBQ=='
    assert a.decode('brMsXW2KwFXaCRO/ZXThBQ==') == 'root'
