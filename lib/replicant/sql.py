import uuid
import sqlite3


REPLICANT_QUEUE = '_replicant_queue'
REPLICANT_LAST_SEQ = '_replicant_last_seq'
REPLICANT_ORIGIN_TABLE = '_replicant_origin'
REPLICANT_ORIGIN_COLUMN = '_replicant_origin'


class SQL:
    def __init__(self, config, schema):
        self.database_uri = config['database_uri']
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
                       % REPLICANT_QUEUE)

        # create replicant last sequency table
        cursor.execute("CREATE TABLE IF NOT EXISTS %s (id INTEGER)"
                       % REPLICANT_LAST_SEQ)

        # create replicant origin table to hold the origin uuid
        cursor.execute("CREATE TABLE IF NOT EXISTS %s (origin TEXT NOT NULL)"
                       % REPLICANT_ORIGIN_TABLE)

        # check if there is an database id already
        result = cursor.execute("SELECT origin FROM %s LIMIT 1"
                                % REPLICANT_ORIGIN_TABLE)
        row = result.fetchone()
        database_id = row['origin'] if row else None
        if not database_id:
            self.database_id = str(uuid.uuid4())
            cursor.execute("INSERT INTO %s (origin) VALUES ('%s')" 
                           % (REPLICANT_ORIGIN_TABLE, self.database_id))

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
                cursor.execute("ALTER TABLE %s ADD COLUMN %s TEXT DEFAULT '%s'"
                               % (table, REPLICANT_ORIGIN_COLUMN, 
                                  self.database_id))

        return True

    def create_triggers(self):
        cursor = self.conn.cursor()

        # create an insert, update and delete trigger for each schema table
        for table in self.schema.keys():
            result = cursor.execute("SELECT name FROM sqlite_master WHERE "
                                    "name=?", (table,))
            if not result.fetchone():
                return False
            
            cursor.execute('''CREATE TRIGGER IF NOT EXISTS _replicant_%s_i 
                AFTER INSERT ON %s 
                WHEN NEW.origin != "%s"
                BEGIN
                    INSERT INTO _replicant_queue (_key, _table, _action) 
                       VALUES (NEW.id, '%s', 'I');
                END;''' % (table, table, self.database_id, table))
            
            cursor.execute('''CREATE TRIGGER IF NOT EXISTS _replicant_%s_u 
                AFTER UPDATE ON %s 
                WHEN NEW.origin != "%s"
                BEGIN
                    INSERT INTO _replicant_queue (_key, _table, _action) 
                       VALUES (NEW.id, '%s', 'U');
                END;''' % (table, table, self.database_id, table))

            cursor.execute('''CREATE TRIGGER IF NOT EXISTS _replicant_%s_d 
                AFTER DELETE ON %s 
                BEGIN
                    INSERT INTO _replicant_queue (_key, _table, _action) 
                       VALUES (OLD.id, '%s', 'D');
                END;''' % (table, table, table))

        return True
