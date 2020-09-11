import unittest

from astra import AstraClient

class TestAstraKeyspaces(unittest.TestCase):
    def setUp(self):
        self.keyspaces = AstraClient.new().keyspaces()

#   @responses.activate
    def test_query(self):
        result = self.keyspaces.insert('whizzy_pops', {'a':'query'})
        result = self.keyspaces.query('whizzy_pops')

        self.assertGreater(result['count'], 0)
    
    def test_query_pk(self):
        result = self.keyspaces.insert('whizzy_pops', {'a':'query_pk'})
        result = self.keyspaces.query_pk('whizzy_pops', ['query_pk'])

        self.assertEqual(result['count'], 1)
        self.assertEqual(result['data'][0]['b'], None)
    
    def test_insert(self):
        result = self.keyspaces.insert('whizzy_pops', {'a':'insert'})

        self.assertEqual(result, {'a':'insert'})

    def test_put(self):
        result = self.keyspaces.insert('whizzy_pops', {'a':'put', 'b': 'first'})
        result = self.keyspaces.query_pk('whizzy_pops', ['put'])

        self.assertEqual(result['data'][0]['b'], 'first')
        
        result = self.keyspaces.put('whizzy_pops', ['put'], {'b': 'put'})
        result = self.keyspaces.query_pk('whizzy_pops', ['put'])

        self.assertEqual(result['data'][0]['b'], 'put')
    
    def test_patch(self):
        result = self.keyspaces.insert('whizzy_pops', {'a':'patch', 'b': 'first'})
        result = self.keyspaces.query_pk('whizzy_pops', ['patch'])

        self.assertEqual(result['data'][0]['b'], 'first')
        
        result = self.keyspaces.patch('whizzy_pops', ['patch'], {'b': 'patch'})
        result = self.keyspaces.query_pk('whizzy_pops', ['patch'])

        self.assertEqual(result['data'][0]['b'], 'patch')
    
    def test_delete(self):
        result = self.keyspaces.insert('whizzy_pops', {'a':'delete', 'b': 'first'})
        result = self.keyspaces.query_pk('whizzy_pops', ['delete'])

        self.assertEqual(result['data'][0]['b'], 'first')

        result = self.keyspaces.delete("whizzy_pops", ['delete'])
        self.assertEqual(result, {})
