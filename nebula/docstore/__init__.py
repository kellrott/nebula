
import os
import uuid
import json
from glob import glob
from urlparse import urlparse, ParseResult
from galaxy.objectstore import ObjectStore, DiskObjectStore
from galaxy.objectstore.local_cache import CachedDiskObjectStore

def from_url(url, **kwds):
    p = urlparse(url)
    if p.scheme in ['', 'filedoc'] :
        return FileDocStore(file_path=p.path, **kwds)

    raise Exception("Unknown ObjectStore %s" % (url))


class DiskObjectStoreConfig:
    def __init__(self, job_work=None, new_file_path=None):
        self.object_store_check_old_style = False
        self.job_working_directory = job_work
        self.new_file_path = new_file_path
        self.umask = 077

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

    def local_cache_base(self):
        return self.objs.local_cache_base()

    def get_url(self):
        raise Exception("Not Implemented")


class TargetDict(dict):
    def __init__(self, src):
        dict.__init__(self, src)
        self.uuid = src['uuid']
        self.id = src.get('id', None)

FILE_SUFFIX=".dat.json"

class FileDocStore(DocStore):
    """
    Cheap and simple file based doc store, not recommended for large systems
    """

    def __init__(self, file_path, cache_path=None, **kwds):
        if cache_path:
            objs = CachedDiskObjectStore(DiskObjectStoreConfig(), cache_path=cache_path, file_path=file_path, **kwds)
        else:
            objs = DiskObjectStore(DiskObjectStoreConfig(), file_path=file_path, **kwds)
        super(FileDocStore, self).__init__(objectstore=objs, **kwds)
        self.file_path = os.path.abspath(file_path)
        self.url = os.path.abspath(self.file_path)
        if not os.path.exists(self.file_path):
            os.mkdir(self.file_path)

    def _docpath(self, id):
        return os.path.join(self.file_path, id[:2], "dataset_" + id + FILE_SUFFIX)

    def _doclist(self):
        return glob(os.path.join(self.file_path, "*", "dataset_*" + FILE_SUFFIX))

    def get(self, id):
        id = self.cleanid(id)
        path = self._docpath(id)
        if not os.path.exists(path):
            return None
        with open(path) as handle:
            data = handle.read()
        return TargetDict(self.loaddoc(data))

    def put(self, id, doc):
        id = self.cleanid(id)
        dir = os.path.join(self.file_path, id[:2])
        if not os.path.exists(dir):
            os.mkdir(dir)
        path =self._docpath(id)
        with open(path, "w") as handle:
            handle.write(self.dumpdoc(doc))

    def filter(self, **kwds):
        for a in self._doclist():
            doc_id = os.path.basename(a).replace(FILE_SUFFIX, "").replace("dataset_", "")
            with open(a) as handle:
                meta = json.loads(handle.read())
                match = True
                for k,v in kwds.items():
                    if k not in meta:
                        match = False
                    else:
                        if isinstance(meta[k], basestring) and meta[k] != v:
                            match = False
                        elif v not in meta[k]:
                            match = False
                if match:
                    yield doc_id, TargetDict(meta)

    def get_url(self):
        return "filedoc://%s" % (self.url)
