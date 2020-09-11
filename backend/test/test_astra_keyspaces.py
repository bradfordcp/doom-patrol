import unittest

import responses
import requests
from astra import AstraClient

class TestAstraKeyspaces(unittest.TestCase):    
#   @responses.activate
    def test_query(self):
        keyspaces = AstraClient.new().keyspaces()
        result = keyspaces.query("whizzy_pops")

        print(result)
        self.assertEqual(result['count'], 2)
    
    def test_insert(self):
        keyspaces = AstraClient.new().keyspaces()
        result = keyspaces.insert("whizzy_pops", {'a':'b'})

        self.assertEqual(result, {'a':'b'})
