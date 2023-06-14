from hashlib import sha256
import base64
from Crypto import Random
from Crypto.Cipher import AES
from datetime import datetime

BS = 16
pad = lambda s: bytes(s + (BS - len(s) % BS) * chr(BS - len(s) % BS), 'utf-8')
unpad = lambda s : s[0:-ord(s[-1:])]

class AESCipher:

    def __init__(self, key):
        self.key = sha256(key.encode('utf-8')).digest()

    def encrypt(self, raw):
        raw = pad(raw)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv) + base64.b64encode(cipher.encrypt(raw))


class CypherString:

    def __init__(self, user):
        self.user = user

    def get_cypher(self, ):
        k = datetime.utcnow().strftime("%Y%m%d")
        cipher = AESCipher(k)
        tag = datetime.utcnow().strftime("%d%m%Y")
        encrypted = cipher.encrypt(f'{self.user}.{tag}')
        return encrypted

    def auth_string(self):
        pw = self.get_cypher()
        s = self.user + ':' + pw.decode("utf-8")
        auth_string = base64.b64encode(s.encode(encoding='UTF-8'))
        return auth_string.decode("utf-8")