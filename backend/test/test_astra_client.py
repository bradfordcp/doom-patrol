import unittest

import responses
import requests
from astra import AstraClient

class TestAstraClient(unittest.TestCase):

  @responses.activate
  def test_auth_token_request(self):
      responses.add(responses.POST, 'https://database-id-region-id.apps.astra.datastax.com/api/rest/v1/auth',
        json={'authToken': 'auth-token'}
      )
      responses.add(responses.GET, 'https://database-id-region-id.apps.astra.datastax.com/api/rest', json={})

      client = AstraClient('database-id', 'region-id', 'username', 'password')
      client.get("")

      self.assertEqual(len(responses.calls), 2)
