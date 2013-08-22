import sqlite3

import couchdb


map_sync = {
    'test1': [
        {'id': 'INTEGER'},
        {'field_a': 'INTEGER'},
        {'field_b': 'TEXT'}
    ]
}

class Replicant(object):
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

        cursor.execute("SELECT id FROM %s WHERE id = '%s'" % (table,
                       _id))
        has_record = cursor.fetchone()
        if change.get('deleted'):
            sql = "DELETE FROM %s WHERE id='%s'" % (table, _id)
            cursor.execute(sql) 
        elif not has_record:
            values = ','.join([ '?' for _ in payload.items() ])
            keys = ','.join([ str(k) for k in payload.keys() ])
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, keys, values)
            cursor.execute(sql, payload.values())
        else:
            values = ', '.join([ '%s=?' % k for k, _ in payload.items() ])
            sql = "UPDATE %s SET %s WHERE id='%s'" % (table, values,
                                                    _id)
            cursor.execute(sql, payload.values())

        self.conn.commit()

    def run(self):
        last_seq = self.get_last_seq()
        changes = self.database.changes(since=last_seq, include_docs=True, style="all_docs")
        for r in changes['results']:
            seq = r.get('seq')
            if seq is None:
                continue

            self.update(r)
            self.set_last_seq(seq)


if __name__ == '__main__':
    r = Replicant()
    r.run()
