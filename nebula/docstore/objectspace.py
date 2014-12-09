
import json
from urllib2 import urlopen
from urlparse import urljoin
from nebula.docstore import DocStore


class ObjectSpaceDoc(DocStore):
    """
    Cheap and simple file based doc store, not recommended for large systems
    """

    def __init__(self, server_url, **kwds):
        super(ObjectSpaceDoc, self).__init__(kwds)
        self.server_url = server_url

    def get(self, id):
        return None

    def put(self, id, doc):
        return None
        
    def filter(self, **kwds):
        results = urlopen( urljoin(self.server_url, "/api/objects") )
        for line in results:
            yield json.loads(line)
