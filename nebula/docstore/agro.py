
import os
import pyagro
from pyagro import agro_pb2
from galaxy.objectstore import directory_hash_id

class AgroDocStore:
    
    def __init__(self, server, file_path):
        self.file_path = os.path.abspath(file_path)
        self.server = server
        self.client = pyagro.AgroClient(self.server)
        self.filestore = self.client.filestore()
    
    def close(self):
        self.client.close()
    
    def get_url(self):
        return "agro://%s" % (self.server)
    
    def local_cache_base(self):
        return self.file_path
    
    def get(self, id):
        doc = self.filestore.GetDoc(agro_pb2.FileID(id=id))
        return pyagro.unpack_doc(doc)

    def put(self, id, doc):
        self.filestore.CreateDoc(pyagro.pack_doc(id, doc))

    def exists(self, obj):
        info = self.filestore.GetFileInfo(agro_pb2.FileID(id=obj.id))
        return info.state == agro_pb2.OK
    
    def create(self, obj):
        pass

    def update_from_file(self, obj, path=None, create=False):
        if path is None:
            path =  os.path.join(self._cache_path_dir(obj), "dataset_%s.dat" % (obj.id))
        if self.exists(obj):
            print "Replacing file"
            self.filestore.DeleteFile(agro_pb2.FileID(id=obj.id))
        pyagro.upload_file(self.filestore, obj.id, path)

    def _cache_path_dir(self, obj):
        return os.path.join(self.file_path, *directory_hash_id(obj.id))

    def get_filename(self, obj):
        obj_dir = self._cache_path_dir(obj)
        if not os.path.exists(obj_dir):
            os.makedirs(obj_dir)
        obj_path = os.path.join(obj_dir, "dataset_%s.dat" % (obj.id))
        pyagro.download_file(self.filestore, obj.id, obj_path)
        return obj_path
