
import pyagro
from pyagro import agro_pb2

class AgroDocStore:
    
    def __init__(self, server):
        self.server = server
        self.client = pyagro.AgroClient(self.server)
        self.filestore = self.client.filestore()
    
    def get(self, id):
        doc = self.filestore.GetDoc(agro_pb2.FileID(id=id))
        print doc
        return pyagro.unpack_doc(doc)

    def put(self, id, doc):
        self.filestore.CreateDoc(pyagro.pack_doc(id, doc))

    