import unittest
import uuid
from astra import AstraClient

class TestAstraDocuments(unittest.TestCase):
    def setUp(self):
        self.documents = AstraClient.new().documents()

    # create, put, get, delete, patch
    def test_create(self):
        document = {
            'foo': 'bar'
        }
        doc_id = self.documents.create("widgets", document)
        result = self.documents.get("widgets", doc_id)

        self.assertEqual(result, document)
    
    def test_put(self):
        doc_id = uuid.uuid4().hex
        document = {
            'foo': 'bar'
        }
        doc_id_out = self.documents.put("widgets", doc_id, document)

        self.assertEqual(doc_id_out, doc_id)

        result = self.documents.get("widgets", doc_id_out)

        self.assertEqual(result, document)
    
    def test_get(self):
        doc_id = uuid.uuid4().hex
        document = {
            'foo': 'bar'
        }
        result = self.documents.put("widgets", doc_id, document)
        result = self.documents.get("widgets", doc_id)

        self.assertEqual(result, document)
    
    def test_patch(self):
        doc_id = uuid.uuid4().hex
        document1 = {
            'foo': 'bar',
            'baz': 'bif'
        }
        document2 = {
            'baz': 'ban'
        }
        self.documents.put("widgets", doc_id, document1)
        doc_id_out = self.documents.patch("widgets", doc_id, document2)

        self.assertEqual(doc_id_out, doc_id)

        result = self.documents.get("widgets", doc_id_out)

        self.assertEqual(result, {'foo':'bar','baz':'ban'})
