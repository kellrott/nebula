
import json
import pymongo
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
        

    def get(self, id):
        return self.doc_collection.find_one({"_id" : id})

    def put(self, id, doc):
        doc['_id'] = id
        self.doc_collection.insert(doc)

    def filter(self, **kwds):
        for doc in self.doc_collection.find(filter=kwds):
            yield doc['_id'], doc

    def exists(self, obj, **kwds):
        raise Exception("Not Implemented")

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
        raise Exception("Not Implemented")

    def get_object_url(self, obj, **kwds):
        raise Exception("Not Implemented")

    def get_store_usage_percent(self):
        raise Exception("Not Implemented")

    def local_cache_base(self):
        raise Exception("Not Implemented")

    def get_url(self):
        raise Exception("Not Implemented")
