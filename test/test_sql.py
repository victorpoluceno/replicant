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
                                   'name=?', (sql.REPLICANT_LOG_TABLE,))
        self.assertNotEqual(result.fetchone(), None)

        result = self.conn.execute('SELECT name FROM sqlite_master WHERE '
                                   'name=?', (sql.REPLICANT_META_TABLE,))
        self.assertNotEqual(result.fetchone(), None)

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
                                   % sql.REPLICANT_LOG_TABLE).fetchall()

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
                                   % sql.REPLICANT_LOG_TABLE).fetchall()
        self.assertEqual(len(result), 3)
        
    def test_queue_size(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()
        self.conn.execute('INSERT INTO test1 (id, field_a, field_b) VALUES '
                          '(1, 1, "x");')
        self.assertEqual(self.sql.queue_size(), 1)

    def test_queue_remove(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()
        self.conn.execute('INSERT INTO test1 (id, field_a, field_b) '
                          'VALUES (1, 1, "x");')
        self.assertEqual(self.sql.queue_remove(), True)
        self.assertEqual(self.sql.queue_size(), 0)
        self.assertEqual(self.sql.queue_remove(), False)

    def test_queue_get(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()
        self.assertEqual(self.sql.queue_get(), None)
        self.conn.execute('INSERT INTO test1 (id, field_a, field_b) '
                  'VALUES (1, 1, "x");')
        self.assertNotEqual(self.sql.queue_get(), None)        

    def test_retrieve_doc(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                  'field_a INTEGER, field_b TEXT)')
        self.conn.execute('INSERT INTO test1 (id, field_a, field_b) '
                  'VALUES (1, 1, "x");')
        self.assertNotEqual(self.sql.retrieve_doc('test1', 1), None)
        self.assertEqual(self.sql.retrieve_doc('test1', 1)['id'], '1')
        self.assertEqual(self.sql.retrieve_doc('test1', 1)['field_a'], 1)

    def test_set_last_seq(self):
        self.sql.initialize()
        self.sql.set_last_seq(1)
        self.assertEqual(self.sql.get_last_seq(), 1)

    def test_get_last_seq(self):
        self.sql.initialize()
        self.assertEqual(self.sql.get_last_seq(), 0)
        self.sql.set_last_seq(1)
        self.sql.set_last_seq(2)
        self.assertEqual(self.sql.get_last_seq(), 2)

    def test_insert_doc(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()
        doc = {'id': '1', 'field_a': 1, 'field_b': 'x'}
        self.assertEqual(self.sql.insert_doc('test1', doc), True)
        result = self.conn.execute('SELECT * FROM test1 WHERE id=?', doc['id'])
        row = result.fetchone()
        self.assertNotEqual(row, None)
        self.assertEqual(row[sql.REPLICANT_ORIGIN_COLUMN], 'remote')
        
        result = self.conn.execute('SELECT * FROM %s'
                                   % sql.REPLICANT_LOG_TABLE)
        row = result.fetchone()   
        self.assertEqual(row, None)

    def test_delete_doc(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()
        doc = {'id': '1', 'field_a': 1, 'field_b': 'x'}
        self.assertEqual(self.sql.insert_doc('test1', doc), True)
        self.assertEqual(self.sql.delete_doc('test1', doc), True)        
        result = self.conn.execute('SELECT * FROM test1 WHERE id=?', doc['id'])
        row = result.fetchone()
        self.assertEqual(row, None)

    def test_update_doc(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                          'field_a INTEGER, field_b TEXT)')
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()
        doc = {'id': '1', 'field_a': 1, 'field_b': 'x'}
        self.assertEqual(self.sql.insert_doc('test1', doc), True)
        doc['field_a'] = 2
        self.assertEqual(self.sql.update_doc('test1', doc), True)
        result = self.conn.execute('SELECT * FROM test1 WHERE id=?', doc['id'])
        row = result.fetchone()
        self.assertNotEqual(row, None)
        self.assertEqual(row['field_a'], 2)
        self.assertEqual(row[sql.REPLICANT_ORIGIN_COLUMN], 'remote')
        result = self.conn.execute('SELECT * FROM %s'
                                   % sql.REPLICANT_LOG_TABLE)
        row = result.fetchone()   
        self.assertEqual(row, None)

    def test_update(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                  'field_a INTEGER, field_b TEXT)')
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()
        doc = {'id': '1', 'field_a': 1, 'field_b': 'x'}
        self.sql.update('test1', doc)
        result = self.conn.execute('SELECT * FROM test1 WHERE id=?', doc['id'])
        row = result.fetchone()
        self.assertNotEqual(row, None)
        doc['field_a'] = 2
        self.sql.update('test1', doc)
        result = self.conn.execute('SELECT * FROM test1 WHERE id=?', doc['id'])
        row = result.fetchone()
        self.assertEqual(row['field_a'], 2)
        self.sql.update('test1', doc, delete=True)
        result = self.conn.execute('SELECT * FROM test1 WHERE id=?', doc['id'])
        row = result.fetchone()
        self.assertEqual(row, None)

    def test_process(self):
        self.conn.execute('CREATE TABLE test1 (id TEXTO PRIMARY KEY, '
                  'field_a INTEGER, field_b TEXT)')
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()
        self.assertEqual(self.sql.process(None), True)
        self.conn.execute('INSERT INTO test1 (id, field_a, field_b) '
                  'VALUES (1, 1, "x");')
        docx = {sql.REPLICANT_ORIGIN_COLUMN: None, 'id': '1', 'field_a': 1, 'field_b': 'x'}
        def callback(table, doc, delete):
            self.assertEqual(table, 'test1')
            self.assertEqual(doc, docx)
            return True
            
        self.assertEqual(self.sql.process(callback), True)
        self.conn.execute('DELETE FROM test1 WHERE id=?', '1')
        def callback(table, doc, delete):
            self.assertEqual(table, 'test1')
            self.assertEqual(doc, None)
            self.assertEqual(delete, True)
            
        self.assertEqual(self.sql.process(callback), True)
        self.assertEqual(self.sql.queue_size(), 1)


if __name__ == '__main__':
    unittest.main()