
import os
import uuid
import json
from glob import glob
from urlparse import urlparse, ParseResult
from nebula.objectstore import ObjectStore, DiskObjectStore, DiskObjectStoreConfig

def init_docstore_url(url):
    p = urlparse(url)
    if p.scheme == '':
        return FileDocStore(file_path=url)

    raise Exception("Unknown ObjectStore %s" % (url))


class DocStore(ObjectStore):
    """
    DocStore abstract interface
    """
    def __init__(self, objectstore, **kwargs):
        self.running = True
        self.objs = objectstore
        self.extra_dirs = {}

    def shutdown(self):
        self.running = False

    def get(self, id):
        raise NotImplementedError()

    def put(self, id, doc):
        raise NotImplementedError()

    def loaddoc(self, data):
        return json.loads(data)

    def dumpdoc(self, doc):
        return json.dumps(doc)

    def cleanid(self, id):
        return str(uuid.UUID(id))

    def filter(self, *kwds):
        raise NotImplementedError()

    """
    ObjectStore methods
    """

    def exists(self, obj, **kwds):
        return self.objs.exists(obj, **kwds)

    def file_ready(self, obj, **kwds):
        return self.objs.file_ready(obj, **kwds)

    def create(self, obj, **kwds):
        return self.objs.create(obj, **kwds)

    def empty(self, obj, **kwds):
        return self.objs.empty(obj, **kwds)

    def size(self, obj, **kwds):
        return self.objs.size(obj, **kwds)

    def delete(self, obj, **kwds):
        return self.objs.delete(obj, **kwds)

    def get_data(self, obj):
        return self.objs.get_data(obj, **kwds)

    def get_filename(self, obj, **kwds):
        return self.objs.get_filename(obj, **kwds)

    def update_from_file(self, obj, file_name=None, create=False, **kwds):
        return self.objs.update_from_file(obj, file_name=file_name, create=create, **kwds)

    def get_object_url(self, obj, **kwds):
        return self.objs.get_object_url(obj, **kwds)

    def get_store_usage_percent(self):
        return self.objs.get_store_usage_percent()

FILE_SUFFIX=".dat.json"

class FileDocStore(DocStore):
    """
    Cheap and simple file based doc store, not recommended for large systems
    """

    def __init__(self, file_path, **kwds):
        objs = DiskObjectStore(DiskObjectStoreConfig(), file_path=file_path)
        super(FileDocStore, self).__init__(objectstore=objs, **kwds)
        self.file_path = file_path
        if not os.path.exists(self.file_path):
            os.mkdir(self.file_path)

    def get(self, id):
        id = self.cleanid(id)
        path = os.path.join(self.file_path, id[:2], id + FILE_SUFFIX)
        if not os.path.exists(path):
            return None
        with open(path) as handle:
            data = handle.read()
        return self.loaddoc(data)

    def put(self, id, doc):
        id = self.cleanid(id)
        dir = os.path.join(self.file_path, id[:2])
        if not os.path.exists(dir):
            os.mkdir(dir)
        path = os.path.join(self.file_path, id[:2], id + FILE_SUFFIX)
        with open(path, "w") as handle:
            handle.write(self.dumpdoc(doc))

    def _doclist(self):
        return glob(os.path.join(self.file_path, "*", "*" + FILE_SUFFIX))

    def filter(self, **kwds):
        for a in self._doclist():
            with open(a) as handle:
                meta = json.loads(handle.read())
                match = True
                for k,v in kwds.items():
                    if meta[k] != v:
                        match = False
                if match:
                    yield meta
