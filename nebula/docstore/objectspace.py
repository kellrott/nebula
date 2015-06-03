
import json
from urllib2 import urlopen
from urlparse import urljoin
from nebula.docstore import DocStore


class ObjectSpace(DocStore):
    """
    Cheap and simple file based doc store, not recommended for large systems
    """

    def __init__(self, server_url, **kwds):
        super(ObjectSpace, self).__init__(kwds)
        self.server_url = server_url

    def get(self, id):
        return None

    def put(self, id, doc):
        return None

    def filter(self, **kwds):
        results = urlopen( urljoin(self.server_url, "/api/docs") )
        for line in results:
            for k, v in json.loads(line).items():
                yield k, v
