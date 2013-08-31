import sqlite3 # TODO lets use gevent
import uuid

import couchdb


map_sync = { # FIXME this shoub sent as a param to a function
    'test1': [
        {'id': 'INTEGER'},
        {'field_a': 'INTEGER'},
        {'field_b': 'TEXT'}
    ]
}


class ReplicantSQLite(object):
    def __init__(self, sqlite_path='./data.db', couchdb_uri='http://127.0.0.1:5984',
                 database_name='data'):
        self.database_name = database_name
        self.couch = couchdb.Server(couchdb_uri)
        if not self.database_name in self.couch:
            self.database = self.couch.create(self.database_name)
        else: 
            self.database = self.couch[self.database_name]
        self.conn = sqlite3.connect(sqlite_path)
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS _replicant_queue 
            (_key INTEGER NOT NULL, _table TEXT, _action TEXT)''')
        self.conn.commit()

        unique = uuid.uuid4()
        for k, v in map_sync.items():
            cursor = self.conn.cursor()
            try:
                cursor.execute('''SELECT origin FROM %s''' % k)
            except:
                cursor.execute('ALTER TABLE %s ADD COLUMN origin TEXT DEFAULT "%s"' % (k, unique))

            cursor.execute('''CREATE TRIGGER IF NOT EXISTS _replicant_%s_i AFTER INSERT ON %s 
                WHEN NEW.origin != "%s"
                BEGIN
                    INSERT INTO _replicant_queue (_key, _table, _action) 
                       VALUES (NEW.id, '%s', 'I');
                END;''' % (k, k, unique, k))
            
            cursor.execute('''CREATE TRIGGER IF NOT EXISTS _replicant_%s_u AFTER UPDATE ON %s 
                WHEN NEW.origin = != "%s"
                BEGIN
                    INSERT INTO _replicant_queue (_key, _table, _action) 
                       VALUES (NEW.id, '%s', 'U');
                END;''' % (k, k, unique, k))

            cursor.execute('''CREATE TRIGGER IF NOT EXISTS _replicant_%s_d AFTER DELETE ON %s 
                BEGIN
                    INSERT INTO _replicant_queue (_key, _table, _action) 
                       VALUES (OLD.id, '%s', 'D');
                END;''' % (k, k, k))
            self.conn.commit()

    def run(self):
        cursor = self.conn.cursor()
        result = cursor.execute('SELECT rowid, _key, _table, _action FROM _replicant_queue ORDER BY rowid LIMIT 1')
        item = result.fetchone()
        if not item:
            return

        result = cursor.execute('SELECT * FROM %s WHERE id=%s' % (item['_table'], item['_key']))
        doc = result.fetchone()
        if not doc:
            return

        payload = dict([ (k, doc[k]) for k in doc.keys() ])
        change = {'payload': payload, '_id': '%s/%s' % (item['_table'], doc['id'])}
        if item['_action'] == 'D':
            change['deleted'] = True

        self.update(change)

        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM _replicant_queue WHERE rowid=%d' % item['rowid'])
        self.conn.commit()

    def update(self, change):
        try:
            r = self.database.get(change['_id'])
        except IndexError:
            r = None

        if not r:
            if change.get('deleted'):
                return

        payload = change['payload']
        self.database[change['_id']]['payload'] = payload # FIXME need to handle the resul here


class ReplicantCouchDB(object):
    def __init__(self, sqlite_path='./data.db',
                 couchdb_uri='http://127.0.0.1:5984', database_name='data'):
        self.database_name = database_name
        self.couch = couchdb.Server(couchdb_uri)
        if not self.database_name in self.couch:
            self.database = self.couch.create(self.database_name)
        else: 
            self.database = self.couch[self.database_name]

        self.conn = sqlite3.connect(sqlite_path)
        cursor = self.conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS last_seq (id INTEGER)')
        self.conn.commit()

    def get_last_seq(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM last_seq order by id desc limit 1')
        seq = cursor.fetchone()
        if not seq:
            return 0

        return seq

    def set_last_seq(self, seq):
        cursor = self.conn.cursor()
        cursor.execute('INSERT INTO last_seq VALUES (%d)' % seq)
        self.conn.commit()

    def update(self, change):
        cursor = self.conn.cursor()
        doc = change['doc']
        table, _id = doc['_id'].split('/')
        payload = doc.get('payload')
        if not table in map_sync:
            return

        if not payload or payload['origin'] != unique:
            return

        cursor.execute("SELECT id FROM %s WHERE id = '%s'" % (table,
                       _id))
        has_record = cursor.fetchone()
        if change.get('deleted'):
            sql = "DELETE FROM %s WHERE id='%s'" % (table, _id)
            cursor.execute(sql) 
        elif not has_record:
            values = ','.join([ '?' for _ in payload.items() ] + ['"remote"'])
            keys = ','.join([ str(k) for k in payload.keys() ] + ['origin'])
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, keys, values)
            cursor.execute(sql, payload.values())
        else:
            values = ', '.join([ '%s=?' % k for k, _ in payload.items() ] + \
                               [ 'origin="remote"'])
            sql = "UPDATE %s SET %s WHERE id='%s'" % (table, values,
                                                    _id)
            cursor.execute(sql, payload.values())

        self.conn.commit()

    def run(self):
        last_seq = self.get_last_seq()
        changes = self.database.changes(since=last_seq, include_docs=True,
                                        style="all_docs")
        for r in changes['results']:
            seq = r.get('seq')
            if seq is None:
                continue

            self.update(r)
            self.set_last_seq(seq)


if __name__ == '__main__':
    # FIXME need to start both at same time

    r = ReplicantCouchDB()
    r.run()

    #rs = ReplicantSQLite()
    #rs.run()
