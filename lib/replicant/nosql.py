import couchdb


class NoSQL:
    def __init__(self, schema, **kwargs):
        self.schema = schema
        self.nosql_uri = kwargs['nosql_uri']
        self.database_name = kwargs['database_name']
        
    def connect(self):
        self.conn = couchdb.Server(self.nosql_uri)
        if not self.database_name in self.conn:
            self.database = self.conn.create(self.database_name)
        else: 
            self.database = self.conn[self.database_name]

        return self.conn

    def load(self, last_seq, callback):
        changes = self.database.changes(since=last_seq, include_docs=True,
                                        style="all_docs")
        for change in changes['results']:
            table, key = change['id'].split('/')

            # if callback return False the transaction was not processed
            # so we must stop here
            if not callback(table, key, doc=change['doc'].get('payload', {}),
                            deleted=change.get('deleted', False)):
                return False

            self.last_seq = change['seq']
        
        return True

    def dump(self, table, key, **kwargs):
        # using a composed key as collection separator
        _id = "%s/%s" % (table, key)

        if kwargs.get('deleted'):
            try:
                del self.database[_id]
            except couchdb.ResourceNotFound:
                return False

            return True
    
        # needed to decided if it is a insert or update operation
        try:
            doc = {}
            doc = self.database.get(_id)
        except IndexError:
            pass

        if not doc:
            self.database[_id] = {'payload': kwargs['payload']}
        else:
            self.database.update(_id, kwargs['payload'])

        return True
