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
        self.nosql = nosql.NoSQL(schema, nosql_uri='http://127.0.0.1:5984',
                                 database_name='data')
        self.conn = self.nosql.connect()
        self.conn.delete('data')

    def test_connect(self):
        self.assertNotEqual(self.nosql.connect(), None)

    def test_load(self):
        self.nosql.connect()
        self.assertEqual(self.nosql.load(0, None), True)
        
        self.nosql.database['test1/1'] = {'payload': {'field_a': 1}}
        def callback(table, key, **kwargs):
            self.assertEqual(table, 'test1')
            self.assertEqual(key, '1')
            self.assertEqual(kwargs['doc'], {'field_a': 1})
            self.assertEqual(kwargs['deleted'], False)

        self.nosql.load(0, callback)

        del self.nosql.database['test1/1']
        def callback(table, key, **kwargs):
            self.assertEqual(kwargs['deleted'], True)

        self.nosql.load(1, callback)

        def callback(table, key, **kargs):
            return False

        self.assertEqual(self.nosql.load(0, callback), False)
        
        def callback(table, key, **kargs):
            return True

        self.assertEqual(self.nosql.load(0, callback), True)

    def test_dump(self):
        self.nosql.connect()
        self.assertEqual(self.nosql.dump('test1', '1', deleted=True), False)


if __name__ == '__main__':
    unittest.main()