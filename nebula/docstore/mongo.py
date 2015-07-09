
import json
import pymongo
import gridfs
from nebula.docstore import DocStore

class MongoStore(DocStore):
    """
    Mongo Based Docstore
    """

    def __init__(self, server_url, **kwds):
        super(MongoStore, self).__init__(None)
        self.server_url = server_url
        client = pymongo.MongoClient(server_url)
        self.db_name = "docstore"
        self.db = client[self.db_name]
        self.doc_collection = self.db['docs']
        self.grid = gridfs.GridFS(self.db)
        

    def get(self, id):
        return self.doc_collection.find_one({"_id" : id})

    def put(self, id, doc):
        doc['_id'] = id
        self.doc_collection.insert(doc)

    def filter(self, **kwds):
        print "KWDS", kwds
        for doc in self.doc_collection.find(kwds):
            print kwds, doc
            yield doc['_id'], doc

    def exists(self, obj, **kwds):
        return self.grid.exists(obj.id)

    def file_ready(self, obj, **kwds):
        raise Exception("Not Implemented")

    def create(self, obj, **kwds):
        raise Exception("Not Implemented")

    def empty(self, obj, **kwds):
        raise Exception("Not Implemented")

    def size(self, obj, **kwds):
        raise Exception("Not Implemented")

    def delete(self, obj, **kwds):
        raise Exception("Not Implemented")

    def get_data(self, obj):
        raise Exception("Not Implemented")

    def get_filename(self, obj, **kwds):
        raise Exception("Not Implemented")

    def update_from_file(self, obj, file_name=None, create=False, **kwds):
        nf = self.grid.new_file(_id=obj.id, chunk_size=5242880)
        with open(file_name) as in_file:
            while 1:
                chunk = in_file.read(1048576)
                if not chunk:
                    break
                nf.write(chunk)
        nf.close()
        
    def get_object_url(self, obj, **kwds):
        raise Exception("Not Implemented")

    def get_store_usage_percent(self):
        raise Exception("Not Implemented")

    def local_cache_base(self):
        raise Exception("Not Implemented")

    def get_url(self):
        raise Exception("Not Implemented")
