import sys
sys.path.append('.')
 
import unittest

from lib.replicant import nosql

schema = {
    'test1': [
        {'id': 'INTEGER'},
        {'field_a': 'INTEGER'},
        {'field_b': 'TEXT'}
    ]
}


class TestNoSQL(unittest.TestCase):
    def setUp(self):
        self.nosql = nosql.NoSQL({'nosql_uri': 'http://127.0.0.1:5984',
                                  'database_name': 'data'}, schema)

    def test_connect(self):
        self.assertNotEqual(self.nosql.connect, None)


if __name__ == '__main__':
    unittest.main()