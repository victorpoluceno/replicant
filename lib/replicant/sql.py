import sqlite3

import gevent


REPLICANT_LOG_TABLE = '_replicant_log'
REPLICANT_META_TABLE = '_replicant_meta'

REPLICANT_ORIGIN_COLUMN = '_replicant_origin'


class SQL:
    def __init__(self, config, schema):
        self.database_uri = config['sql_uri']
        self.schema = schema

    def connect(self):
        self.conn = sqlite3.connect(self.database_uri)
        self.conn.row_factory = sqlite3.Row
        return self.conn

    def initialize(self):        
        with self.conn as conn:
            # create replicant log table
            conn.execute("CREATE TABLE IF NOT EXISTS %s "
                         "(_key INTEGER NOT NULL, _table TEXT, _action TEXT)"
                         % REPLICANT_LOG_TABLE)

            # create replicant meta table and the first row
            conn.execute("CREATE TABLE IF NOT EXISTS %s (last_seq INTEGER)"
                         % REPLICANT_META_TABLE)
            conn.execute("INSERT INTO %s (last_seq) VALUES (0)"
                         % REPLICANT_META_TABLE)

    def alter_schema(self):
        # create origin field for every table on schema
        with self.conn as conn:
            for table in self.schema.keys():
                result = conn.execute("SELECT name FROM sqlite_master WHERE "
                                        "name=?", (table,))
                if not result.fetchone():
                    return False

                try:
                    # check if origin field already exists
                    conn.execute("SELECT %s FROM %s"
                                   % (REPLICANT_ORIGIN_COLUMN, table))
                except sqlite3.OperationalError:
                    # add column origin field
                    conn.execute("ALTER TABLE %s ADD COLUMN %s TEXT;"
                                   % (table, REPLICANT_ORIGIN_COLUMN))

        return True

    def create_triggers(self):
        with self.conn as conn:
            # create an insert, update and delete trigger for each schema table
            for table in self.schema.keys():
                result = conn.execute("SELECT name FROM sqlite_master WHERE "
                                        "name=?", (table,))
                if not result.fetchone():
                    return False

                stmt = '''CREATE TRIGGER IF NOT EXISTS _replicant_%s_i 
                    AFTER INSERT ON %s 
                    WHEN NEW.%s ISNULL
                    BEGIN
                        INSERT INTO %s (_key, _table, _action) 
                           VALUES (NEW.id, '%s', 'I');
                    END;''' % (table, table, REPLICANT_ORIGIN_COLUMN,
                               REPLICANT_LOG_TABLE, table)
                conn.execute(stmt)
                
                stmt = '''CREATE TRIGGER IF NOT EXISTS _replicant_%s_u 
                    AFTER UPDATE ON %s 
                    WHEN NEW.%s ISNULL
                    BEGIN
                        INSERT INTO %s (_key, _table, _action) 
                           VALUES (NEW.id, '%s', 'U');
                    END;''' % (table, table, REPLICANT_ORIGIN_COLUMN,
                               REPLICANT_LOG_TABLE, table)
                conn.execute(stmt)

                stmt = '''CREATE TRIGGER IF NOT EXISTS _replicant_%s_d 
                    AFTER DELETE ON %s 
                    BEGIN
                        INSERT INTO %s (_key, _table, _action) 
                           VALUES (OLD.id, '%s', 'D');
                    END;''' % (table, table, REPLICANT_LOG_TABLE, table)
                conn.execute(stmt)

        return True

    def queue_get(self):
        result = self.conn.execute("SELECT rowid, _key, _table, _action FROM %s "
                                "ORDER BY rowid LIMIT 1"
                                % REPLICANT_LOG_TABLE)
        row = result.fetchone() if result else None
        if not row: # nothing on the log
            return None

        return row

    def queue_size(self):
        result = self.conn.execute("SELECT count(*) FROM %s "
                                % REPLICANT_LOG_TABLE)
        return result.fetchone()[0] if result else 0
        
    def queue_remove(self):
        result = self.conn.execute("SELECT rowid, _key, _table, _action FROM %s "
                                "ORDER BY rowid LIMIT 1"
                                % REPLICANT_LOG_TABLE)
        row = result.fetchone() if result else None
        if not row: # nothing on the log
            return False

        self.conn.execute("DELETE FROM %s WHERE rowid=%d"
                       % (REPLICANT_LOG_TABLE, row['rowid']))

        return True

    def retrieve_doc(self, table, key):
        result = self.conn.execute("SELECT * FROM %s WHERE id=?" % table, 
                                                                     (key,))
        return result.fetchone() if result else None
       
    def set_last_seq(self, seq):
        self.conn.execute('UPDATE %s SET last_seq=%d'
                       % (REPLICANT_META_TABLE, seq))

    def get_last_seq(self):
        result = self.conn.execute('SELECT last_seq FROM %s'
                                % REPLICANT_META_TABLE)
        row = result.fetchone()
        return row['last_seq'] if row else None

    def insert_doc(self, table, doc):
        keys = ','.join([ str(k) for k in doc.keys() ] + 
                        [REPLICANT_ORIGIN_COLUMN])
        values = ','.join([ '?' for _ in range(len(doc)+1) ])
        stmt = 'INSERT INTO %s (%s) VALUES (%s)' % (table, keys, values)
        self.conn.execute(stmt, doc.values() + ['remote'])
        return True

    def update_doc(self, table, doc):
        values = ', '.join([ '%s=?' % k for k in doc.keys() 
                           + [REPLICANT_ORIGIN_COLUMN] ])
        stmt = "UPDATE %s SET %s WHERE id=?" % (table, values)
        self.conn.execute(stmt, doc.values() + ['remote', doc['id']])
        return True

    def delete_doc(self, table, doc):
        self.conn.execute('DELETE FROM %s WHERE id=?' % table, (doc['id'],))
        return True

    def update(self, table, doc, delete=False):
        if delete:
            self.delete_doc(table, doc)
        else:
            # check if it exists first
            if self.retrieve_doc(table, doc['id']):
                self.update_doc(table, doc)
            else:
                self.insert_doc(table, doc)

    def process(self, callback):
        size = self.queue_size()
        while size > 0:
            item = self.queue_get()
            doc = self.retrieve_doc(item['_table'], item['_key'])
            if doc:
                doc = dict(doc)

            delete = False
            if item['_action'] == 'D':
                delete = True

            if callback(item['_table'], doc, delete):
                self.queue_remove()
            
            size -= 1

        return True
