import json
import unittest
import unittest.mock as mock
from api.api import jackson

class TestEndpoints(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestEndpoints, self).__init__(*args, **kwargs)
        self.test_client = jackson().test_client()
        self.test_client.testing = True

    def test_spoof(self):
        response = self.test_client.get("/api/spoof_get_events/")
        assert response.status_code == 200