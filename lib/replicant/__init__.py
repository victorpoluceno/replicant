from gevent import monkey; monkey.patch_all()

from lib.replicant import sql
from lib.replicant import nosql


class Replicant:
    def __init__(self, config, schema):
        self.sql = sql.SQL(config, schema)
        self.sql.connect()
        self.sql.initialize()
        self.sql.alter_schema()
        self.sql.create_triggers()

        self.nosql = nosql.NoSQL(config, schema)
        self.nosql.connect()

    def run(self):
        seq = self.sql.get_last_seq()
        last_seq = self.nosql.process(self.sql.update, seq)
        if last_seq:    
            self.sql.set_last_seq(last_seq)
        
        self.sql.process(self.nosql.update)

