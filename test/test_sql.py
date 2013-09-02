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
        self.sql = sql.SQL({'sql_uri': ':memory:'}, schema)
        self.conn = self.sql.connect()

    def test_connect(self):
        self.assertNotEqual(self.sql.connect, None)

    def test_initialize(self):
        self.sql.initialize()
        result = self.conn.execute('SELECT name FROM sqlite_master WHERE '
                                   'name=?', (sql.REPLICANT_QUEUE_TABLE,))
        self.assertNotEqual(result.fetchone(), None)

        result = self.conn.execute('SELECT name FROM sqlite_master WHERE '
                                   'name=?', (sql.REPLICANT_CONFIG_TABLE,))
        self.assertNotEqual(result.fetchone(), None)

        result = self.conn.execute('SELECT origin FROM %s LIMIT 1'
                                   % sql.REPLICANT_CONFIG_TABLE)
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
        self.assertEqual(self.sql.create_triggers(), False)

        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        self.assertEqual(self.sql.create_triggers(), True)

        for table in schema.keys():
            result = self.conn.execute('SELECT name FROM sqlite_master WHERE '
                                       'name=?', ('_replicant_%s_i' % table,))
            self.assertNotEqual(result.fetchone(), None)

            result = self.conn.execute('SELECT name FROM sqlite_master WHERE '
                                       'name=?', ('_replicant_%s_u' % table,))
            self.assertNotEqual(result.fetchone(), None)

            result = self.conn.execute('SELECT name FROM sqlite_master WHERE '
                                       'name=?', ('_replicant_%s_d' % table,))
            self.assertNotEqual(result.fetchone(), None)

    def test_replicant_triggers(self):
        self.sql.initialize()
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        self.sql.alter_schema()
        self.sql.create_triggers()
        self.conn.execute('INSERT INTO test1 (id, field_a, field_b) '
                          'VALUES (1, 1, "x");')
        self.conn.execute('UPDATE test1 SET field_b="y";')
        self.conn.execute('DELETE FROM test1 WHERE id=1;')
        result = self.conn.execute('SELECT * FROM %s ORDER BY rowid'
                                   % sql.REPLICANT_QUEUE_TABLE).fetchall()

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['_table'], 'test1')
        self.assertEqual(result[0]['_action'], 'I')
        self.assertEqual(result[0]['_key'], 1)

        self.assertEqual(result[1]['_table'], 'test1')
        self.assertEqual(result[1]['_action'], 'U')
        self.assertEqual(result[1]['_key'], 1)

        self.assertEqual(result[2]['_table'], 'test1')
        self.assertEqual(result[2]['_action'], 'D')
        self.assertEqual(result[2]['_key'], 1)

        self.conn.execute('INSERT INTO test1 (id, field_a, field_b, '
                          '_replicant_origin) VALUES (1, 1, "y", "remote");')
        result = self.conn.execute('SELECT * FROM %s ORDER BY rowid'
                                   % sql.REPLICANT_QUEUE_TABLE).fetchall()
        self.assertEqual(len(result), 3)
        
    def test_replicant_queue_size(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()
        self.conn.execute('INSERT INTO test1 (id, field_a, field_b) VALUES '
                          '(1, 1, "x");')
        self.assertEqual(self.sql.queue_size(), 1)

    def test_replicant_queue_remove(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()
        self.conn.execute('INSERT INTO test1 (id, field_a, field_b) '
                          'VALUES (1, 1, "x");')
        
        def queue_remove_callback(row):
            self.assertNotEqual(row, None)
        self.assertEqual(self.sql.queue_remove(queue_remove_callback), True)
        self.assertEqual(self.sql.queue_size(), 1)

        def queue_remove_callback(row):
            self.assertNotEqual(row, None)
            return True
        self.assertEqual(self.sql.queue_remove(queue_remove_callback), True)
        self.assertEqual(self.sql.queue_size(), 0)
        self.assertEqual(self.sql.queue_remove(None), False)

    def test_retrieve_doc(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                  'field_a INTEGER, field_b TEXT)')
        self.conn.execute('INSERT INTO test1 (id, field_a, field_b) '
                  'VALUES (1, 1, "x");')
        self.assertNotEqual(self.sql.retrieve_doc('test1', 1), None)
        self.assertEqual(self.sql.retrieve_doc('test1', 1)['id'], '1')
        self.assertEqual(self.sql.retrieve_doc('test1', 1)['field_a'], 1)

    def test_save_last_seq(self):
        self.sql.initialize()
        self.sql.save_last_seq(1)
        self.assertEqual(self.sql.get_last_seq(), 1)

    def test_get_last_seq(self):
        self.sql.initialize()
        self.assertEqual(self.sql.get_last_seq(), None)
        self.sql.save_last_seq(1)
        self.sql.save_last_seq(2)
        self.assertEqual(self.sql.get_last_seq(), 2)


if __name__ == '__main__':
    unittest.main()