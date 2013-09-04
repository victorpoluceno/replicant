import couchdb


class NoSQL:
    def __init__(self, config, schema):
        self.nosql_uri = config['nosql_uri']
        self.database_name = config['database_name']
        self.schema = schema
        
    def connect(self):
        self.conn = couchdb.Server(self.nosql_uri)
        if not self.database_name in self.conn:
            self.database = self.conn.create(self.database_name)
        else: 
            self.database = self.conn[self.database_name]

        return self.conn

    def process(self, callback, last_seq):
        changes = self.database.changes(since=last_seq, include_docs=True,
                                        style="all_docs")
        for r in changes['results']:
            seq = r.get('seq')
            if seq is None:
                continue

            table, key = r['id'].split('/')
            payload = r.get('payload', {})
            delete = r.get('deleted', False)
            if delete:
                payload['id'] = key

            if not callback(table, payload, delete):
                break
            
        return seq

    def update(self, table, doc, delete):
        _id = "%s/%s" % (table, doc['id'])
        try:
            old = self.database.get(_id)
        except IndexError:
            if delete:
                del self.database[_id]
            else:
                self.database[_id] = {'payload': doc}
        else:
            new = old.update(doc)
            self.database[_id] = new
