import couchdb


class NoSQL:
    def __init__(self, config, schema):
        self.database_uri = config['nosql_uri']
        self.database_name = config['database_name']
        self.schema = schema
        
    def connect(self):
        self.conn = couchdb.Server(self.nosql_uri)
        if not self.database_name in self.conn:
            self.database = self.conn.create(self.database_name)
        else: 
            self.database = self.conn[self.database_name]

        return self.conn