import sys
sys.path.append('.')
 
import unittest

from lib.replicant import sql

schema = {
    'test1': [
        {'id': 'INTEGER'},
        {'field_a': 'INTEGER'},
        {'field_b': 'TEXT'}
    ]
}


class TestSQL(unittest.TestCase):
    def setUp(self):
        self.sql = sql.SQL({'database_uri': ':memory:'}, schema)
        self.conn = self.sql.connect()

    def test_connect(self):
        self.assertNotEqual(self.sql.connect, None)

    def test_initialize(self):
        self.sql.initialize()
        result = self.conn.execute('SELECT name FROM sqlite_master WHERE name=?',
                          (sql.REPLICANT_QUEUE,))
        self.assertNotEqual(result.fetchone(), None)

        result = self.conn.execute('SELECT name FROM sqlite_master WHERE name=?',
                          (sql.REPLICANT_LAST_SEQ,))
        self.assertNotEqual(result.fetchone(), None)

        result = self.conn.execute('SELECT name FROM sqlite_master WHERE name=?',
                          (sql.REPLICANT_ORIGIN_TABLE,))
        self.assertNotEqual(result.fetchone(), None)

        result = self.conn.execute('SELECT origin FROM %s LIMIT 1'
                                   % sql.REPLICANT_ORIGIN_TABLE)
        self.assertEqual(result.fetchone()['origin'], self.sql.database_id)  

    def test_alter_schema(self):
        self.sql.initialize()

        # schema table does not exists yet
        self.assertEqual(self.sql.alter_schema(), False)

        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                'field_a INTEGER, field_b TEXT)')
        self.assertEqual(self.sql.alter_schema(), True)

        for table in schema.keys():
            result = self.conn.execute('SELECT %s FROM %s LIMIT 1' 
                                       % (sql.REPLICANT_ORIGIN_COLUMN, table))
            self.assertEqual(result.fetchone(), None)

    def test_create_triggers(self):
        self.sql.initialize()

        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        self.assertEqual(self.sql.create_triggers(), True)

        for table in schema.keys():
            result = self.conn.execute('SELECT name FROM sqlite_master WHERE name=?',
                          ('_replicant_%s_i' % table,))
            self.assertNotEqual(result.fetchone(), None)

            result = self.conn.execute('SELECT name FROM sqlite_master WHERE name=?',
                          ('_replicant_%s_u' % table,))
            self.assertNotEqual(result.fetchone(), None)

            result = self.conn.execute('SELECT name FROM sqlite_master WHERE name=?',
                          ('_replicant_%s_d' % table,))
            self.assertNotEqual(result.fetchone(), None)


if __name__ == '__main__':
    unittest.main()