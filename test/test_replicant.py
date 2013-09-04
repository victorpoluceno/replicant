import sys
sys.path.append('.')
 
import unittest

from lib.replicant import Replicant

schema = {
    'test1': [
        {'id': 'INTEGER'},
        {'field_a': 'INTEGER'},
        {'field_b': 'TEXT'}
    ]
}


class TestReplicant(unittest.TestCase):
    def setUp(self):
        self.replicant = Replicant({'sql_uri': ':memory:',
                                    'nosql_uri': 'http://127.0.0.1:5984',
                                    'database_name': 'data'}, schema)

    def test_run(self):
        self.replicant.sql.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        self.replicant.run()


if __name__ == '__main__':
    unittest.main()