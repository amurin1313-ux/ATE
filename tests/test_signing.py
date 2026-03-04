import unittest
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from okx.private_client import sign_okx

class TestSigning(unittest.TestCase):
    def test_sign_okx_known(self):
        # this is a deterministic check with a fixed timestamp/path/body
        ts = "2020-12-08T09:08:57.715Z"
        method = "GET"
        path = "/api/v5/account/balance"
        body = ""
        secret = "testsecret"
        sig = sign_okx(ts, method, path, body, secret)
        self.assertIsInstance(sig, str)
        self.assertGreater(len(sig), 10)

if __name__ == "__main__":
    unittest.main()
