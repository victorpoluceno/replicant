import uuid
import sqlite3


REPLICANT_QUEUE_TABLE = '_replicant_queue'
REPLICANT_CONFIG_TABLE = '_replicant_config'
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
        cursor = self.conn.cursor()
        
        # create replicant queue table
        cursor.execute("CREATE TABLE IF NOT EXISTS %s "
                       "(_key INTEGER NOT NULL, _table TEXT, _action TEXT)"
                       % REPLICANT_QUEUE_TABLE)

        # create replicant config table to hold replicant config
        cursor.execute("CREATE TABLE IF NOT EXISTS %s (origin TEXT NOT NULL,"
                       "last_seq INTEGER)" % REPLICANT_CONFIG_TABLE)

        # check if there is an database id already
        result = cursor.execute("SELECT origin FROM %s LIMIT 1"
                                % REPLICANT_CONFIG_TABLE)
        row = result.fetchone()
        database_id = row['origin'] if row else None
        if not database_id:
            self.database_id = str(uuid.uuid4())
            cursor.execute("INSERT INTO %s (origin) VALUES ('%s')" 
                           % (REPLICANT_CONFIG_TABLE, self.database_id))

    def alter_schema(self):
        cursor = self.conn.cursor()

        # create origin field for every table on schema
        for table in self.schema.keys():
            result = cursor.execute("SELECT name FROM sqlite_master WHERE "
                                    "name=?", (table,))
            if not result.fetchone():
                return False

            try:
                # check if origin field already exists
                cursor.execute("SELECT %s FROM %s"
                               % (REPLICANT_ORIGIN_COLUMN, table))
            except sqlite3.OperationalError:
                # add column origin field
                cursor.execute("ALTER TABLE %s ADD COLUMN %s TEXT;"
                               % (table, REPLICANT_ORIGIN_COLUMN))

        return True

    def create_triggers(self):
        cursor = self.conn.cursor()

        # create an insert, update and delete trigger for each schema table
        for table in self.schema.keys():
            result = cursor.execute("SELECT name FROM sqlite_master WHERE "
                                    "name=?", (table,))
            if not result.fetchone():
                return False

            stmt = '''CREATE TRIGGER IF NOT EXISTS _replicant_%s_i 
                AFTER INSERT ON %s 
                WHEN NEW.%s ISNULL
                BEGIN
                    INSERT INTO _replicant_queue (_key, _table, _action) 
                       VALUES (NEW.id, '%s', 'I');
                END;''' % (table, table, REPLICANT_ORIGIN_COLUMN, table)
            cursor.execute(stmt)
            
            cursor.execute('''CREATE TRIGGER IF NOT EXISTS _replicant_%s_u 
                AFTER UPDATE ON %s 
                WHEN NEW.%s ISNULL
                BEGIN
                    INSERT INTO _replicant_queue (_key, _table, _action) 
                       VALUES (NEW.id, '%s', 'U');
                END;''' % (table, table, REPLICANT_ORIGIN_COLUMN, table))

            cursor.execute('''CREATE TRIGGER IF NOT EXISTS _replicant_%s_d 
                AFTER DELETE ON %s 
                BEGIN
                    INSERT INTO _replicant_queue (_key, _table, _action) 
                       VALUES (OLD.id, '%s', 'D');
                END;''' % (table, table, table))

        return True

    def queue_size(self):
        cursor = self.conn.cursor()
        result = cursor.execute("SELECT count(*) FROM %s "
                                % REPLICANT_QUEUE_TABLE)
        return result.fetchone()[0] if result else 0
        
    def queue_remove(self, callback):
        cursor = self.conn.cursor()
        result = cursor.execute("SELECT rowid, _key, _table, _action FROM %s "
                                "ORDER BY rowid LIMIT 1"
                                % REPLICANT_QUEUE_TABLE)
        row = result.fetchone() if result else None
        if not row:
            return False

        if callback(row):
            cursor.execute("DELETE FROM %s WHERE rowid=%d"
                           % (REPLICANT_QUEUE_TABLE, row['rowid']))

        return True

    def retrieve_doc(self, table, key):
        cursor = self.conn.cursor()
        result = cursor.execute("SELECT * FROM %s WHERE id=%d" % (table, key))
        return result.fetchone() if result else None
       
    def save_last_seq(self, seq):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE %s SET last_seq=%d'
                       % (REPLICANT_CONFIG_TABLE, seq))

        self.conn.commit()

    def get_last_seq(self):
        cursor = self.conn.cursor()
        result = cursor.execute('SELECT last_seq FROM %s'
                                % REPLICANT_CONFIG_TABLE)
        return result.fetchone()['last_seq'] if result else None

    def insert_doc(self, table, doc):
        keys = ','.join([ str(k) for k in doc.keys() ] + 
                        [REPLICANT_ORIGIN_COLUMN])
        values = ','.join([ '?' for _ in range(len(doc)+1) ])
        stmt = 'INSERT INTO %s (%s) VALUES (%s)' % (table, keys, values)
        self.conn.execute(stmt, doc.values() + ['remote'])
        return True
